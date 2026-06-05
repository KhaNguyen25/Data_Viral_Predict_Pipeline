package edu.hust.edgededuplicate.core;

import edu.hust.edgededuplicate.redis.RedisUtility;
import edu.hust.edgededuplicate.utill.*;

import java.util.*;
import java.util.logging.Logger;

public class EdgeServer {
    private static final Logger logger = GlobalLogger.getLogger();

    private final String filterClass = ConfigurationManager.getProperty("filterClass");
    private final int hopNum = ConfigurationManager.getIntProperty("hopNum");
    public final int dataVolume = ConfigurationManager.getRandomIntArrayProperty("dataVolumeOptions");
    private final String deduplicateStrategy = ConfigurationManager.getProperty("deduplicateStrategy");

    private final boolean aiProtectEnabled =
            Boolean.parseBoolean(ConfigurationManager.getProperty("ai.protect.enabled"));

    public int serverID;
    public Map<Integer, NeighborServerInfo> neighborInfoMap = new HashMap<>();//附近server信息
    public HierarchicalTree IndexTree;//索引树
    public List<Long> dataTable = new ArrayList<>();//本地数据存储

    public EdgeServer(int serverID) {
        this.serverID = serverID;
    }

    public void createNeighborServerInfo(int[][] distancesMatrix, Map<Integer, EdgeServer> serversMap) {
        for (Map.Entry<Integer, EdgeServer> entry : serversMap.entrySet()) {
            int targetServerID = entry.getKey();
            EdgeServer targetServer = entry.getValue();
            int hop = calculateServerDistance(distancesMatrix, serverID, targetServerID);
            if (hop <= hopNum && serverID != targetServerID) {
                if (targetServer != null) {
                    NeighborServerInfo neighborInfo = new NeighborServerInfo(
                            targetServer.serverID,
                            targetServer.dataVolume,
                            hop
                    );
                    neighborInfoMap.put(targetServer.serverID, neighborInfo);
                }
            }
        }
    }

    private int calculateServerDistance(int[][] distance, int source, int destination) {
        return distance[source][destination];
    }

    private int calculateDataDistance(long data, List<Integer> filteredList, EdgeServer otherServer) {
        int bestHop = Integer.MAX_VALUE;
        if (otherServer.searchLocalData(data)) {
            return 0;
        } else {
            List<Integer> otherServerNeighborHaveData = otherServer.searchIndexTreeAllReturnID(data);
            for (Integer neighborHaveData : otherServerNeighborHaveData) {
                if (!filteredList.contains(neighborHaveData)) {
                    int neighborHaveDataHop = otherServer.neighborInfoMap.get(neighborHaveData).hop;
                    if (neighborHaveDataHop <= bestHop) {
                        bestHop = neighborHaveDataHop;
                    }
                }
            }
            return bestHop;
        }

    }

    public Map<Integer, Integer> getNeighborServersVolume() {
        Map<Integer, Integer> neighborServersVolume = new HashMap<>();
        for (NeighborServerInfo neighborInfo : neighborInfoMap.values()) {
            neighborServersVolume.put(neighborInfo.serverID, neighborInfo.serverDataVolume);
        }
        return neighborServersVolume;
    }

    public void createIndexTree(int[][] adjMatrix) {
        if (Objects.equals(filterClass, "CountingBloomFilter")) {
            this.IndexTree = new CBFilterHTree(adjMatrix, this.serverID);
        } else if (Objects.equals(filterClass, "AutoExpandQuotientFilter")) {
            this.IndexTree = new AEQFilterHTree(adjMatrix, this.serverID, getNeighborServersVolume());
        }
    }


    /**
     *
     * @return Server storage utilization
     */
    public double getStorageUtilization() {
        return (this.dataTable.size() + 0.0) / this.dataVolume;
    }


    /**
     * @param mes new a UpdateMes class as input value
     */
    public void updateIndex(UpdateMessage mes) {
        if (mes.transHop > hopNum) {
            logger.warning("[Server " + serverID + "] Unable to update a server that is not in the hop limit from" + mes.serverID + "'s update message");
            return;
        }
        if (Objects.equals(mes.type, "insert")) {
            IndexTree.insert(mes.dataHash, mes.serverID, mes.transHop);
            // logger.info("[Server " + serverID + "] get insert message from server " + mes.serverID);
        } else if (Objects.equals(mes.type, "delete")) {
            IndexTree.delete(mes.dataHash, mes.serverID, mes.transHop);
            // logger.info("[Server " + serverID + "] get delete message from server " + mes.serverID);
        }else {
            logger.warning("[Server " + serverID + "] Only insert and delete operation is supported");
        }
    }

    /**
     * only query the root filter
     * @param hash data hash
     * @return boolean
     */
    public boolean searchIndexTreeRoot(long hash) {
        return this.IndexTree.searchRoot(hash);
    }

    /**
     * query the level node of hop
     * @param hash data hash
     * @param hopNum only search server at level
     * @return boolean
     */
    public boolean searchIndexTreeRoot2Hop(long hash, int hopNum) {
        if (hopNum > this.hopNum) {
            logger.warning("[Server " + serverID + "]Error: hopNum out of the limit!");
            return false;
        }
        else {
            return this.IndexTree.searchHop(hash, hopNum);
        }
    }
    /**
     * get all severs with the data for the specified number of hop
     * @param hash data hash
     * @param hopNum only search server at level
     * @return List of serverID
     */
    public List<Integer> searchIndexTreeOnlyHopReturnID(long hash, int hopNum) {
        if (hopNum > this.hopNum) {
            logger.warning("[Server " + serverID + "] Error: hopNum out of the limit!");
            return null;
        }
        else {
            return this.IndexTree.searchOnlyHopReturnID(hash, hopNum);
        }
    }

    /**
     * get all severs with the data
     * @param hash data hash
     * @return List of serverID
     */
    public List<Integer> searchIndexTreeAllReturnID(long hash) {
        return this.IndexTree.searchAllReturnID(hash);
    }


    //Local Data management
    /**
     * Query whether data exists locally
     * @param data data hash
     * @return boolean
     */
    public boolean searchLocalData(long data) {
        return dataTable.stream().anyMatch(e -> e.equals(data));
    }

    /**
     * Insert a piece of data that does not exist to the server
     * @param data data hash
     */
//    public boolean insertLocalData(long data) {
//        if (dataTable.size() >= dataVolume) {
//            logger.warning("[Server " + serverID + "] server storage limit reached!");
//            return false;
//        } else if (searchLocalData(data)) {
//            logger.warning("[Server " + serverID + "] data " + data + " exist!");
//            return false;
//        } else {
//            dataTable.add(data);
//            return true;
//        }
//    }

    public boolean insertLocalData(long data) {
        if (searchLocalData(data)) {
            return false;
        }

        if (dataTable.size() >= dataVolume) {
            logger.warning("[Server " + serverID + "] server storage limit reached!");
            return false;
        }

        dataTable.add(data);
        return true;
    }


    /**
     * insert a data to the server
     *
     * @param data data hash
     */
    public boolean deleteLocalData(long data) {
        ExperimentRecord.allDelete++;
        if (!searchLocalData(data)) {
            logger.warning("[Server " + serverID + "] server can not find data " + data);
            return false;
        }
        else {

            dataTable.remove(data);
            return true;
        }
    }

    public List<Long> findSame(List<Long> duplicateDataList) {
        List<Long> matchingData = new ArrayList<>();
        for (Long data : dataTable) {
            if (duplicateDataList.contains(data)) {
                matchingData.add(data);
            }
        }
        return matchingData;
    }


//    public List<Boolean> getPermission(List<Long> duplicateDataList, Map<Integer, EdgeServer> serversMap) {
//        List<List<Boolean>> permissionList = new ArrayList<>();
//        for (Map.Entry<Integer, NeighborServerInfo> entry : neighborInfoMap.entrySet()) {
//            int neighborServerID = entry.getKey();
//            EdgeServer neighborServer = serversMap.get(neighborServerID);
//            List<Boolean> duplicatePermissionList = new ArrayList<>();
//            for (Long data : duplicateDataList) {
//                if (neighborServer.searchIndexTreeAllReturnID(data).size() <= 1) {
//                    duplicatePermissionList.add(false);
//                } else {
//                    duplicatePermissionList.add(true);
//                }
//            }
//            permissionList.add(duplicatePermissionList);
//        }
//        List<Boolean> permission = new ArrayList<>();
//        for (int i = 0; i < duplicateDataList.size(); i++) {
//            boolean canDelete = true;
//            for (List<Boolean> subList : permissionList) {
//                if (!subList.get(i)) {
//                    canDelete = false;
//                    break;
//                }
//            }
//            permission.add(canDelete);
//        }
//        return permission;
//    }

    public List<Boolean> getPermission(List<Long> duplicateDataList, Map<Integer, EdgeServer> serversMap) {
        /*
         * Ý nghĩa:
         * Hàm này xác định object nào trong duplicateDataList được phép xóa khỏi node hiện tại.
         *
         * Logic giống Ripple gốc:
         * - Không duyệt toàn bộ serversMap.
         * - Chỉ duyệt các server lân cận nằm trong neighborInfoMap.
         * - neighborInfoMap đã được tạo trước đó theo điều kiện hop <= hopNum.
         * - Vì vậy, việc kiểm tra quyền xóa có xét phạm vi hop một cách gián tiếp.
         *
         * Kết quả:
         * - permission.get(i) = true  => object duplicateDataList.get(i) được phép xóa.
         * - permission.get(i) = false => không được xóa object đó.
         */

        List<List<Boolean>> permissionList = new ArrayList<>();

        // Duyệt từng neighbor của server hiện tại.
        // neighborInfoMap chỉ chứa các server nằm trong phạm vi hopNum.
        for (Map.Entry<Integer, NeighborServerInfo> entry : neighborInfoMap.entrySet()) {
            int neighborServerID = entry.getKey();
            EdgeServer neighborServer = serversMap.get(neighborServerID);

            if (neighborServer == null) {
                continue;
            }

            List<Boolean> duplicatePermissionList = new ArrayList<>();

            for (Long data : duplicateDataList) {
                /*
                 * searchIndexTreeAllReturnID(data) trả về danh sách server mà neighbor biết
                 * là đang có object này.
                 *
                 * Nếu số server chứa object <= 1:
                 *   Không nên xóa, vì object có thể không còn bản sao đủ an toàn.
                 *
                 * Nếu số server chứa object > 1:
                 *   Có thể xóa ở node hiện tại vì object vẫn còn được lưu ở nơi khác
                 *   trong phạm vi mà neighbor biết.
                 */
                if (neighborServer.searchIndexTreeAllReturnID(data).size() <= 1) {
                    duplicatePermissionList.add(false);
                } else {
                    duplicatePermissionList.add(true);
                }
            }

            permissionList.add(duplicatePermissionList);
        }

        List<Boolean> permission = new ArrayList<>();

        /*
         * Tổng hợp kết quả từ tất cả neighbor.
         *
         * Một object chỉ được phép xóa nếu tất cả neighbor đều cho phép.
         * Nếu chỉ cần một neighbor trả false, object đó không được xóa.
         */
        for (int i = 0; i < duplicateDataList.size(); i++) {
            boolean canDelete = true;

            for (List<Boolean> subList : permissionList) {
                if (!subList.get(i)) {
                    canDelete = false;
                    break;
                }
            }

            permission.add(canDelete);
        }

        return permission;
    }

    public void deduplicate(int[][] distancesMatrix, Map<Integer, EdgeServer> serversMap, Map<Integer, List<EdgeClient>> clientsMap) {

        ExperimentRecord.dupSum++;
        System.out.println("[DEDUP-STRATEGY] Server " + serverID + " strategy=" + deduplicateStrategy);
        List<Long> deepCopyDataTable = new ArrayList<>(dataTable);
        if (Objects.equals(deduplicateStrategy, "basic")) {
            // basic策略
            // 获得可能删除数据的list
            List<Long> duplicateDataList = new ArrayList<>();
            for (Long data : deepCopyDataTable) {
                if (searchIndexTreeRoot(data)) {
                    duplicateDataList.add(data);
                }
            }
//            Code kiểm tra
            System.out.println(
                    "[DEDUP-DEBUG] Server " + serverID +
                            " | dataTable=" + dataTable.size() +
                            " | duplicateCandidates=" + duplicateDataList.size()
            );
            //TODO
            // 次数 duplicateDataList.size()
            // 距离
            ExperimentRecord.packSum += duplicateDataList.size();
            for (Map.Entry<Integer, NeighborServerInfo> entry : neighborInfoMap.entrySet()) {
                int hop = entry.getValue().hop;
                ExperimentRecord.transHops+=hop;
            }


            // 获得permission
            List<Boolean> permission = getPermission(duplicateDataList, serversMap);

            //kiem tra
            int allowedDelete = 0;
            for (Boolean p : permission) {
                if (p) {
                    allowedDelete++;
                }
            }


            System.out.println(
                    "[DEDUP-DEBUG] Server " + serverID +
                            " | candidates=" + duplicateDataList.size() +
                            " | allowedDelete=" + allowedDelete
            );
            // 删除许可的数据
//            for (int i = 0; i < duplicateDataList.size(); i++) {
//                Long data = duplicateDataList.get(i);
//                if (permission.get(i)) {
//                    if (RedisUtility.isVideoViral(data.intValue())) {
//                        logger.info("[Smart-Dedup] Bỏ qua xóa video viral: " + data);
//                        continue; // Bỏ qua vòng lặp này, không thực hiện deleteLocalData
//                    }
//                    deleteLocalData(data);
//                    // server通知附近服务器更新索引树
//                    for (Map.Entry<Integer, NeighborServerInfo> entry : neighborInfoMap.entrySet()) {
//                        int neighborServerID = entry.getKey();
//                        int hop = entry.getValue().hop;
//                        EdgeServer neighborServer = serversMap.get(neighborServerID);
//                        UpdateMessage updateMessage = new UpdateMessage("delete", serverID, data, hop);
//                        neighborServer.updateIndex(updateMessage);
//                    }
//                }
//            }
//            Code test
            int realDeleted = 0;
            int viralSkipped = 0;

// 删除许可的数据
            //Xóa tất cả các duplicate được cho phép
            // 1. Lập danh sách candidate thật sự có quyền xóa
            Set<Long> candidateIds = new HashSet<>();

            for (int i = 0; i < duplicateDataList.size(); i++) {
                if (permission.get(i)) {
                    candidateIds.add(duplicateDataList.get(i));
                }
            }

// 2. Gọi Redis MGET một lần để lấy danh sách object viral
            Set<Long> viralCandidateSet = Collections.emptySet();

            if (aiProtectEnabled && !candidateIds.isEmpty()) {
                long redisStartNs = System.nanoTime();

                viralCandidateSet = RedisUtility.getViralVideoIds(candidateIds);

                long redisEndNs = System.nanoTime();

                System.out.println(
                        "[Smart-Dedup][MGET] Server=" + serverID
                                + ", candidates=" + duplicateDataList.size()
                                + ", deletableCandidates=" + candidateIds.size()
                                + ", viralFound=" + viralCandidateSet.size()
                                + ", redisMgetTimeMs=" + String.format("%.3f", (redisEndNs - redisStartNs) / 1_000_000.0)
                );
            }

// 3. Duyệt danh sách xóa như cũ, nhưng không gọi Redis GET từng object nữa
            for (int i = 0; i < duplicateDataList.size(); i++) {
                Long data = duplicateDataList.get(i);

                if (permission.get(i)) {
                    if (aiProtectEnabled && viralCandidateSet.contains(data)) {
                        viralSkipped++;

                        // Khi benchmark, không nên log từng object vì có thể làm chậm hệ thống.
                        // logger.info("[Smart-Dedup] Bỏ qua xóa video viral: " + data);
                        continue;
                    }

                    boolean deleted = deleteLocalData(data);

                    if (deleted) {
                        realDeleted++;
                        MetricCollector.recordEviction(data, MetricCollector.currentLineNumber);

                        // server通知附近服务器更新索引树
                        for (Map.Entry<Integer, NeighborServerInfo> entry : neighborInfoMap.entrySet()) {
                            int neighborServerID = entry.getKey();
                            int hop = entry.getValue().hop;
                            EdgeServer neighborServer = serversMap.get(neighborServerID);

                            if (neighborServer == null) {
                                continue;
                            }

                            UpdateMessage updateMessage =
                                    new UpdateMessage("delete", serverID, data, hop);

                            neighborServer.updateIndex(updateMessage);
                        }
                    }
                }
            }


//            Xóa ở ngưỡn 80%(giống bài báo)
//            double storageUpperLimit = ConfigurationManager.getDoubleProperty("storageUpperLimit");
//            int targetSize = (int) Math.floor(dataVolume * storageUpperLimit);
//            int maxDeleteNeeded = Math.max(0, dataTable.size() - targetSize);
//
//            for (int i = 0; i < duplicateDataList.size(); i++) {
//                if (realDeleted >= maxDeleteNeeded) {
//                    break;
//                }
//
//                Long data = duplicateDataList.get(i);
//
//                if (permission.get(i)) {
//                    if (RedisUtility.isVideoViral(data)) {
//                        viralSkipped++;
//                        logger.info("[Smart-Dedup] Bỏ qua xóa video viral: " + data);
//                        continue;
//                    }
//
//                    boolean deleted = deleteLocalData(data);
//
//                    if (deleted) {
//                        realDeleted++;
//
//                        for (Map.Entry<Integer, NeighborServerInfo> entry : neighborInfoMap.entrySet()) {
//                            int neighborServerID = entry.getKey();
//                            int hop = entry.getValue().hop;
//                            EdgeServer neighborServer = serversMap.get(neighborServerID);
//
//                            if (neighborServer == null) {
//                                continue;
//                            }
//
//                            UpdateMessage updateMessage =
//                                    new UpdateMessage("delete", serverID, data, hop);
//
//                            neighborServer.updateIndex(updateMessage);
//                        }
//                    }
//                }
//            }

            System.out.println(
                    "[DEDUP-DEBUG] Server " + serverID +
                            " | realDeleted=" + realDeleted +
                            " | viralSkipped=" + viralSkipped +
                            " | storageAfter=" + String.format("%.6f", getStorageUtilization())
            );


        } else if (deduplicateStrategy.startsWith("latency")) {
            List<Long> duplicateDataList = new ArrayList<>();
            Set<Integer> serverSetIDSaveData = new HashSet<>();
            // 可能删除的数据
            for (Long data : deepCopyDataTable) {
                List<Integer> serversIDSave = searchIndexTreeAllReturnID(data);
                if (!serversIDSave.isEmpty()) {
                    duplicateDataList.add(data);
                    serverSetIDSaveData.addAll(serversIDSave);
                }
            }
            //TODO
            // 次数 duplicateDataList.size()
            // 距离
            ExperimentRecord.packSum+=duplicateDataList.size();
            for (Map.Entry<Integer, NeighborServerInfo> entry : neighborInfoMap.entrySet()) {
                int serverID = entry.getKey();
                if (serverSetIDSaveData.contains(serverID)) {
                    int hop = entry.getValue().hop;
                    ExperimentRecord.transHops +=hop;
                }

            }


            // 获得数据删除许可
            Map<Long, List<Integer>> dataPermission = new HashMap<>();
            for (Integer serverID : serverSetIDSaveData) {
                EdgeServer server = serversMap.get(serverID);
                List<Long> sameData = server.findSame(duplicateDataList);
                List<Boolean> serverPermission = server.getPermission(sameData, serversMap);

                //TODO
                // 次数 sameData.size()
                // 距离
                ExperimentRecord.packSum+=sameData.size();
                for (Map.Entry<Integer, NeighborServerInfo> entry : server.neighborInfoMap.entrySet()) {
                    int hop = entry.getValue().hop;
                    ExperimentRecord.transHops +=hop;
                }

                // 遍历serverPermission列表
                for (int i = 0; i < sameData.size(); i++) {
                    Long data = sameData.get(i);
                    Boolean permission = serverPermission.get(i);
                    if(permission){
                        if (dataPermission.containsKey(data)) {
                            // 如果数据已经在dataPermission中，只有当权限为false时才更新为false
                            dataPermission.get(data).add(serverID);
                        } else {
                            // 如果数据不在dataPermission中，直接添加
                            List<Integer> deleteSever = new ArrayList<>();
                            deleteSever.add(serverID);
                            dataPermission.put(data, deleteSever);
                        }
                    }

                }
            }

            //计算延迟 删除数据
            List<Integer> allServerID = new ArrayList<>(neighborInfoMap.keySet());
            allServerID.add(serverID);


            Set<Long> viralCandidateSet = Collections.emptySet();

            if (aiProtectEnabled && !dataPermission.isEmpty()) {
                long redisStartNs = System.nanoTime();

                viralCandidateSet = RedisUtility.getViralVideoIds(dataPermission.keySet());

                long redisEndNs = System.nanoTime();

                System.out.println(
                        "[Smart-Dedup][MGET-Latency] Server=" + serverID
                                + ", candidates=" + dataPermission.size()
                                + ", viralFound=" + viralCandidateSet.size()
                                + ", redisMgetTimeMs=" + String.format("%.3f", (redisEndNs - redisStartNs) / 1_000_000.0)
                );
            }

            for (Map.Entry<Long, List<Integer>> entry : dataPermission.entrySet()) {
                // 计算延迟
                List<Integer> serversIDSaveData = entry.getValue();
                int deleteStrategy = -1;
                double highestLatency = Double.MIN_VALUE;
                for (Integer serverID : serversIDSaveData) {
                    // 计算仅保留该服务器对局部的延迟
                    double averageLatency = 0;
                    for (Integer eachServerID : allServerID) {
                        averageLatency += calculateServerDistance(distancesMatrix, eachServerID, serverID);
                    }
                    averageLatency /= allServerID.size();
                    if (averageLatency >= highestLatency) {
                        highestLatency = averageLatency;
                        deleteStrategy = serverID;
                    }
                }

                Long videoHash = entry.getKey();
                // --- KIỂM TRA TRƯỚC KHI YÊU CẦU SERVER XÓA ---
                if (aiProtectEnabled && viralCandidateSet.contains(videoHash)) {
                    logger.info("[Smart-Dedup] Video " + videoHash + " đang viral. Hủy lệnh xóa để bảo vệ băng thông.");
                    continue;
                }
                //需要删除的服务器
                EdgeServer serverNeedDelete = serversMap.get(deleteStrategy);
                serverNeedDelete.deleteLocalData(entry.getKey());
                for (Map.Entry<Integer, NeighborServerInfo> neighborServerInfoEntry : serverNeedDelete.neighborInfoMap.entrySet()) {
                    int neighborServerID = neighborServerInfoEntry.getKey();
                    int hop = neighborServerInfoEntry.getValue().hop;
                    EdgeServer neighborServer = serversMap.get(neighborServerID);
                    UpdateMessage updateMessage = new UpdateMessage("delete", serverNeedDelete.serverID, entry.getKey(), hop);
                    neighborServer.updateIndex(updateMessage);
                }

            }
        }
    }

    /**
     *
     * @param testData
     * @return
     */
    public double testRootFPR(List<Long> testData){
        return IndexTree.testRootFPR(testData);
    }

    /**
     *
     * @param testData
     * @return
     */
    public double testLevelFPR(List<Long> testData){
        return IndexTree.testLevelFPR(testData);
    }

    /**
     *
     * @param testData
     * @return
     */
    public double testAllFPR(List<Long> testData){
        return IndexTree.testAllFPR(testData);
    }

    public long getAllMemory(){
        return IndexTree.getAllMemory();
    }
}
