package edu.hust.edgededuplicate.core;

import edu.hust.edgededuplicate.utill.ConfigurationManager;
import edu.hust.edgededuplicate.utill.ExperimentRecord;
import edu.hust.edgededuplicate.utill.GlobalLogger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class Main {
    private static final Logger logger = GlobalLogger.getLogger();
    private static final int epoch = ConfigurationManager.getIntProperty("epoch");
    private static final double probability = ConfigurationManager.getDoubleProperty("probability");
    private static final double storageUpperLimit = ConfigurationManager.getDoubleProperty("storageUpperLimit");
    private static final String requestLogPath = ConfigurationManager.getProperty("requestLogPath");

    public static void main(String[] args) throws IOException {
        for (int exp = 0; exp < 1; exp++) {
            logger.severe("=".repeat(40) + "Initialization" + "=".repeat(40));
            // 1. 生成随机连通图
//            Tạo đồ thị liên thông
            RandomConnectedGraph graph = new RandomConnectedGraph();
            graph.generateGraph();
            // 2. 生成servers
//            Tạo máy chủ
            Map<Integer, EdgeServer> serversMap = new HashMap<>();
            for (int serverID = 0; serverID < graph.getNumServers(); serverID++) {
                EdgeServer server = new EdgeServer(serverID);
                serversMap.put(serverID, server);
            }
            // 3.server初始化邻居信息和创建索引树
//            Khởi tạo thông tin về các máy chủ lân cận và tạo cây chỉ mục.
            for (Map.Entry<Integer, EdgeServer> entry : serversMap.entrySet()) {
                EdgeServer server = entry.getValue();
                server.createNeighborServerInfo(graph.getDistancesMatrix(), serversMap);
                server.createIndexTree(graph.getAdjacencyMatrix());

                logger.info("[Server " + server.serverID + "] complete initialization");
            }

//            Khởi tạo ReaLTimeController
            RealtimeSendController controller =
                    new RealtimeSendController(serversMap, graph.getNumServers(),graph.getDistancesMatrix());
//          Gọi hàm đọc log và đẩy request vào node
            controller.startStreaming(requestLogPath);




            // 3. GOM CHỈ SỐ VÀ IN RA MÀN HÌNH CHUẨN FORM

            List<Double> FPRList = new ArrayList<>();
            FPRList.add(-1.0);

            List<Long> MemoryList = new ArrayList<>();
            MemoryList.add(-1L);

            ExperimentRecord.Latency = -1;

            ExperimentRecord experimentRecord = new ExperimentRecord(FPRList, MemoryList);



            System.out.println("\n" + "=".repeat(20) + " BÁO CÁO KẾT QUẢ RIPPLE " + "=".repeat(20));

            System.out.println("1. Tổng số Log/Video đã thêm (allAdd)  : " + ExperimentRecord.allAdd);
            System.out.println("2. Tổng số Video đã xóa (allDelete)    : " + ExperimentRecord.allDelete);
            System.out.println("3. Tỷ lệ xóa trùng lặp (dedupRate)     : " +
                    String.format("%.6f", (ExperimentRecord.allAdd == 0
                            ? 0.0
                            : (ExperimentRecord.allDelete * 1.0 / ExperimentRecord.allAdd))));
            System.out.println("4. Số lần gọi deduplicate              : " + ExperimentRecord.dupSum);

            // Xuất ra file log của hệ thống
            logger.info(experimentRecord.toString());
            experimentRecord.moveLog();
            ExperimentRecord.clear();



//
//            logger.severe("=".repeat(80));
//            // 3. 开始模拟
//            List<Double> FPRList = new ArrayList<>();
//            List<Long> MemoryList = new ArrayList<>();
//            for (int i = 0; i < epoch; i++) {
////                logger.severe("-".repeat(40) + "epoch: " + i + "-".repeat(40));
//                //4. 生成client请求
////                Tạo yêu cầu dữ liệu
//                // logger.info("Generate Random " + graph.getNumClients() + " clients");
//                Map<Integer, List<EdgeClient>> clientsMap = new HashMap<>();
//                for (int clientID = 0; clientID < graph.getNumClients(); clientID++) {
//                    EdgeClient client = new EdgeClient(clientID, graph.generateRandomServerID(), graph.generateRealData());
//                    clientsMap.computeIfAbsent(client.getServerIDCovered(), k -> new ArrayList<>()).add(client);
//                }
//                double totalFPR = 0;
//                long totalMemory = 0;
//                //5. server插入数据并去重
////               **** Máy chủ chèn dữ liệu và loại bỏ các bản ghi trùng lặp.
//                for (Map.Entry<Integer, EdgeServer> serverEntry : serversMap.entrySet()) {
//                    EdgeServer server = serverEntry.getValue();
////                    Mô phỏng dữ liệu(code gốc)
////                    if (graph.generateRandomDouble() <= probability) {
////                        // 5.1 生成单条随机数据
////                        long data = graph.generateRealData();
////                        // 5.2 本地插入数据
////                        if (server.insertLocalData(data)) {
////                            // 5.3 遍历相邻server更新索引
////                            // logger.info("[Server "+ server.serverID + "] insert data " + data);
////                            ExperimentRecord.allAdd++;
////                            for (Map.Entry<Integer, NeighborServerInfo> entry : server.neighborInfoMap.entrySet()) {
////                                int neighborServerID = entry.getKey();
////                                int hop = entry.getValue().hop;
////                                EdgeServer neighborServer = serversMap.get(neighborServerID);
////                                UpdateMessage updateMessage = new UpdateMessage("insert", server.serverID, data, hop);
////                                neighborServer.updateIndex(updateMessage);
////                            }
////                        }
////                    }
//                    // logger.info("[Server "+ server.serverID + "] StorageUtilization: " + server.getStorageUtilization());
//                    // 5.4 判断是否达到总容量并更新索引树
//                    if (server.getStorageUtilization() >= storageUpperLimit) {
//                        logger.warning("[Server " + server.serverID + "] deduplicate!");
//                        server.deduplicate(graph.getDistancesMatrix(), serversMap, clientsMap);
//                    }
//                    // 5.5 假阳性测试
//                    List<Long> fakeDataArray = graph.generateFakeDataArray();
//                    totalFPR += server.testRootFPR(fakeDataArray);
//                    totalMemory += server.getAllMemory();
////                    logger.info("[Server " + server.serverID + "] FPR: " + totalFPR + " Memory: " + totalMemory);
//                }
//                //延迟测试
//                double averageLatency = 0;
//                for (Map.Entry<Integer, List<EdgeClient>> clientEntry : clientsMap.entrySet()) {
//                    EdgeServer server = serversMap.get(clientEntry.getKey());
//                    List<EdgeClient> clientList = clientEntry.getValue();
//                    double averageLatency4server = 0;
//                    for (EdgeClient client : clientList) {
//                        int bestDistance = Integer.MAX_VALUE;
//                        long data = client.getClientRequestData();
//                        if (server.searchLocalData(data)) {
//                            bestDistance = 0;
//                        } else {
//                            List<Integer> serverList = new ArrayList<>(server.neighborInfoMap.keySet());
//                            boolean flag = false;
//                            for (Integer eachServerID : serverList) {
//                                EdgeServer eachServer = serversMap.get(eachServerID);
//                                if (eachServer.searchLocalData(data)) {
//                                    flag = true;
//                                    int distance = server.neighborInfoMap.get(eachServerID).hop;
//                                    if (distance <= bestDistance) {
//                                        bestDistance = distance;
//                                    }
//                                }
//                            }
//                            if (!flag) {
//                                bestDistance = ConfigurationManager.getIntProperty("CloudHop");
//                            }
//                        }
//                        averageLatency4server += bestDistance;
//                    }
//                    averageLatency4server /= clientList.size();
//                    averageLatency += averageLatency4server;
//                }
//                averageLatency /= serversMap.size();
//                ExperimentRecord.Latency = averageLatency;
//
//                FPRList.add(totalFPR/serversMap.size());
//                MemoryList.add(totalMemory/serversMap.size());
//
////                if((epoch+1)%200 == 0){
////                    for (Map.Entry<Integer, EdgeServer> serverEntry : serversMap.entrySet()) {
////                        EdgeServer server = serverEntry.getValue();
////                        if (graph.generateRandomDouble() <= probability) {
////                            server.clearHalf();
////                        }
////                    }
////                }
//
//            }
//            ExperimentRecord experimentRecord = new ExperimentRecord(FPRList, MemoryList);
//            logger.info(experimentRecord.toString());
//            System.out.println(ExperimentRecord.allAdd);
//            System.out.println(ExperimentRecord.allDelete);
//            experimentRecord.moveLog();
//            System.out.println(ExperimentRecord.dupSum);
//            ExperimentRecord.clear();
//        }

        }
    }
}
