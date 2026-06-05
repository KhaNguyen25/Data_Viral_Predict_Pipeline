package edu.hust.edgededuplicate.core;

import edu.hust.edgededuplicate.core.NeighborServerInfo;
import edu.hust.edgededuplicate.kafka.KafkaRequestProducer;
import edu.hust.edgededuplicate.utill.ConfigurationManager;
import edu.hust.edgededuplicate.utill.ExperimentRecord;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import com.google.common.util.concurrent.RateLimiter;


public class RealtimeSendController {

    private final Map<Integer, EdgeServer> serversMap;
    private final int totalNodes;
    private final KafkaRequestProducer kafkaProducer;
    private final int[][] distancesMatrix;
    private final double storageUpperLimit;
    private final double simSpeed;
    private long simStartWallClockMs = -1;
    private static final java.util.logging.Logger logger =
            edu.hust.edgededuplicate.utill.GlobalLogger.getLogger();


    private final int maxTestLines;
    private final int dedupBatchSize;
    private final int maxGetRequests;

    private final RateLimiter rateLimiter;
    private final int testDurationMinutes;
    private final long testDurationMs;






    public RealtimeSendController(Map<Integer, EdgeServer> serversMap, int totalNodes, int[][] distancesMatrix) {
        this.serversMap = serversMap;
        this.totalNodes = totalNodes;
        this.kafkaProducer = new KafkaRequestProducer();

        this.distancesMatrix = distancesMatrix;
        this.storageUpperLimit = ConfigurationManager.getDoubleProperty("storageUpperLimit");
        this.simSpeed = ConfigurationManager.getDoubleProperty("simSpeed");

        this.maxTestLines = ConfigurationManager.getIntProperty("maxTestLines");
        this.dedupBatchSize = ConfigurationManager.getIntProperty("dedupBatchSize");


        double logRatePerSecond = ConfigurationManager.getDoubleProperty("logRatePerSecond");
        this.rateLimiter = RateLimiter.create(logRatePerSecond);

        this.testDurationMinutes = ConfigurationManager.getIntProperty("testDurationMinutes");
        this.testDurationMs = this.testDurationMinutes * 60L * 1000L;
        this.maxGetRequests = ConfigurationManager.getIntProperty("maxGetRequests");

    }


    public void startStreaming(String filePath) {

        System.out.println("Bắt đầu nạp dữ liệu từ file log: " + filePath);
        this.simStartWallClockMs = System.currentTimeMillis();

        boolean stoppedByLineLimit = false;
        boolean stoppedByTimeLimit = false;

        long startMs = System.currentTimeMillis();

        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(new GZIPInputStream(new FileInputStream(filePath))))) {

            String line;
            int lineNumber = 0;
            int getRequestCount = 0;

            boolean alreadyDedupAtThisLine = false;

            while ((line = br.readLine()) != null) {
                lineNumber++;

                if (line.trim().isEmpty()) {
                    continue;
                }

                if (lineNumber == 1 && !Character.isDigit(line.trim().charAt(0))) {
                    continue;
                }

                boolean isGetRequest = processLine(line, lineNumber);

                if (isGetRequest) {
                    getRequestCount++;
                }

                long elapsedMs = System.currentTimeMillis() - startMs;

                // Cứ mỗi dedupBatchSize GET request, lấy mẫu mức sử dụng cache.
                // Mẫu này dùng để tính Avg Storage Utilization trong MetricCollector.
                if (isGetRequest && dedupBatchSize > 0 && getRequestCount % this.dedupBatchSize == 0) {
                    MetricCollector.sampleStorage(serversMap);
                }

                alreadyDedupAtThisLine = false;

                if (isGetRequest && dedupBatchSize > 0 && getRequestCount % dedupBatchSize == 0) {
                    runBatchDedup(getRequestCount);
                    alreadyDedupAtThisLine = true;
                }

                if (lineNumber % 10_000 == 0) {
                    double seconds = elapsedMs / 1000.0;
                    double minutes = seconds / 60.0;
                    double speed = lineNumber / Math.max(seconds, 0.001);

                    System.out.println(
                            "[SPEED] line=" + lineNumber +
                                    ", getRequests=" + getRequestCount +
                                    ", elapsed=" + String.format("%.2f", seconds) + "s" +
                                    ", elapsedMin=" + String.format("%.2f", minutes) +
                                    ", speed=" + String.format("%.2f", speed) + " lines/s"
                    );
                }

                // Dừng theo thời gian chạy, ví dụ 30 phút
                if (testDurationMs > 0 && elapsedMs >= testDurationMs) {
                    System.out.println(
                            "[STOP] Đã chạy đủ " + testDurationMinutes +
                                    " phút, dừng streaming theo thời gian."
                    );
                    stoppedByTimeLimit = true;
                    break;
                }
//                Dừng theo maxrequest>0
                if (maxGetRequests > 0 && getRequestCount >= maxGetRequests) {
                    System.out.println(
                            "[STOP] Đã chạy đủ " + maxGetRequests +
                                    " request GET hợp lệ, dừng streaming."
                    );
                    stoppedByLineLimit = true;
                    break;
                }

                // Dừng theo số dòng nếu maxTestLines > 0
                // Nếu maxTestLines = 0 thì bỏ qua điều kiện này
                if (maxTestLines > 0 && lineNumber >= maxTestLines) {
                    System.out.println(
                            "[STOP] Đã chạy đủ " + maxTestLines +
                                    " dòng test, dừng streaming theo số dòng."
                    );
                    stoppedByLineLimit = true;
                    break;
                }
            }

            // Chạy dedup thêm một lần cuối sau khi dừng batch
            if (!alreadyDedupAtThisLine) {
                runBatchDedup(getRequestCount);
            }

            long totalElapsedMs = System.currentTimeMillis() - startMs;
            double totalSeconds = totalElapsedMs / 1000.0;
            double totalMinutes = totalSeconds / 60.0;
            double avgSpeed = lineNumber / Math.max(totalSeconds, 0.001);



            System.out.println(
                    "[SUMMARY] processedLines=" + lineNumber +
                            ", getRequests=" + getRequestCount +
                            ", elapsed=" + String.format("%.2f", totalSeconds) + "s" +
                            ", elapsedMin=" + String.format("%.2f", totalMinutes) +
                            ", avgSpeed=" + String.format("%.2f", avgSpeed) + " lines/s" +
                            ", stoppedByTimeLimit=" + stoppedByTimeLimit +
                            ", stoppedByLineLimit=" + stoppedByLineLimit
            );

            if (stoppedByTimeLimit) {
                System.out.println("Đã dừng theo giới hạn thời gian, chưa nạp hết toàn bộ file log.");
            } else if (stoppedByLineLimit) {
                System.out.println("Đã dừng theo giới hạn số dòng test, chưa nạp hết toàn bộ file log.");
            } else {
                System.out.println("Đã nạp xong toàn bộ file log trước khi đạt giới hạn thời gian.");
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            kafkaProducer.close();
        }
    }


    private void runBatchDedup(int lineNumber) {
        System.out.println("\n[BATCH-DEDUP] Start at line=" + lineNumber);

        int checkedServers = 0;
        int dedupServers = 0;
        int totalDeletedOnCallerNodes = 0;

        for (Map.Entry<Integer, EdgeServer> entry : serversMap.entrySet()) {
            EdgeServer server = entry.getValue();
            checkedServers++;

            if (server.getStorageUtilization() >= this.storageUpperLimit) {
                dedupServers++;

                int sizeBefore = server.dataTable.size();
                double storageBefore = server.getStorageUtilization();

                System.out.println(
                        "[BATCH-DEDUP-BEFORE] NODE_" + server.serverID +
                                " size=" + sizeBefore +
                                ", storage=" + String.format("%.6f", storageBefore)
                );

                server.deduplicate(this.distancesMatrix, serversMap, new HashMap<>());

                int sizeAfter = server.dataTable.size();
                double storageAfter = server.getStorageUtilization();
                int deleted = sizeBefore - sizeAfter;
                totalDeletedOnCallerNodes += Math.max(deleted, 0);

                System.out.println(
                        "[BATCH-DEDUP-DONE] NODE_" + server.serverID +
                                " size before=" + sizeBefore +
                                ", after=" + sizeAfter +
                                ", deleted=" + deleted +
                                ", storage before=" + String.format("%.6f", storageBefore) +
                                ", after=" + String.format("%.6f", storageAfter)
                );
            }
        }

        System.out.println(
                "[BATCH-DEDUP] Done at line=" + lineNumber +
                        ", checkedServers=" + checkedServers +
                        ", dedupServers=" + dedupServers +
                        ", deletedOnCallerNodes=" + totalDeletedOnCallerNodes +
                        "\n"
        );
    }

//    private void processLine(String line, int lineNumber) {
//
//        MetricCollector.currentLineNumber = lineNumber;
//        rateLimiter.acquire();
//        // Dùng được cả log cách nhau bằng khoảng trắng hoặc dấu phẩy
//        String[] columns = line.trim().split("[,\\s]+");
//
//        if (columns.length < 6) {
//            System.err.println("[Log " + lineNumber + "] Bỏ qua vì không đủ cột: " + line);
//            return;
//        }
//
//        try {
//            /*
//             * Theo mô tả của bạn:
//             * - Cột thứ 5, index 4, là giá trị dùng để chia dư cho 30.
//             * - Giá trị đó cũng nên được dùng làm videoId/dataHash trong Ripple.
//             */
//            long timestampRel = System.currentTimeMillis();
//
//            String videoId = columns[1].trim();
//            long dataHash = convertVideoIdToLong(videoId);
//
//            long nodeNumber = Long.parseLong(columns[4].trim());
//            int nodeId = (int) Math.floorMod(nodeNumber, totalNodes);
//
//
////            String operation = columns[5];
//            String operation = columns[5].trim().toLowerCase();
//
//
//
////            waitUntilLogTime(timestampRel);
//
//            EdgeServer targetServer = serversMap.get(nodeId);
//
//            if (targetServer == null) {
//                System.err.println("[Log " + lineNumber + "] Không tìm thấy node " + nodeId);
//                return;
//            }
//
//
//            if (!"get".equals(operation)) {
//                sendKafkaEvent(timestampRel, dataHash, nodeId, operation);
//                return;
//            }
//
//            int distance = MetricCollector.measureDistance(dataHash, targetServer, serversMap);
//            MetricCollector.recordRequest(dataHash, lineNumber, distance);
//
//            boolean existedBefore = targetServer.searchLocalData(dataHash);
//            boolean inserted = false;
//
////            if (!existedBefore && targetServer.getStorageUtilization() >= this.storageUpperLimit) {
////                int sizeBefore = targetServer.dataTable.size();
////                double storageBefore = targetServer.getStorageUtilization();
////
////                System.out.println(
////                        "[DEDUP-BEFORE-INSERT] NODE_" + (nodeId+1) +
////                                " storage=" + String.format("%.4f", storageBefore) +
////                                " >= " + this.storageUpperLimit +
////                                ", start deduplicate..."
////                );
////
////                targetServer.deduplicate(this.distancesMatrix, serversMap, new HashMap<>());
////
////                int sizeAfter = targetServer.dataTable.size();
////                double storageAfter = targetServer.getStorageUtilization();
////                int deleted = sizeBefore - sizeAfter;
////
////                System.out.println(
////                        "[DEDUP-DONE] NODE_" + (nodeId+1) +
////                                " size before=" + sizeBefore +
////                                ", after=" + sizeAfter +
////                                ", deleted=" + deleted +
////                                ", storage before=" + String.format("%.6f", storageBefore) +
////                                ", after=" + String.format("%.6f", storageAfter)
////                );
////            }
//
//
//
//
//
//            if (!existedBefore) {
//                if (targetServer.getStorageUtilization() >= this.storageUpperLimit) {
//                    int sizeBefore = targetServer.dataTable.size();
//                    double storageBefore = targetServer.getStorageUtilization();
//
//                    System.out.println(
//                            "[DEDUP-BEFORE-INSERT] NODE_" + targetServer.serverID +
//                                    " size=" + sizeBefore +
//                                    ", storage=" + String.format("%.6f", storageBefore) +
//                                    " >= " + this.storageUpperLimit
//                    );
//
//                    targetServer.deduplicate(this.distancesMatrix, serversMap, new HashMap<>());
//
//                    int sizeAfter = targetServer.dataTable.size();
//                    double storageAfter = targetServer.getStorageUtilization();
//
//                    System.out.println(
//                            "[DEDUP-AFTER-INSERT] NODE_" + targetServer.serverID +
//                                    " size before=" + sizeBefore +
//                                    ", after=" + sizeAfter +
//                                    ", deleted=" + (sizeBefore - sizeAfter) +
//                                    ", storage after=" + String.format("%.6f", storageAfter)
//                    );
//                }
//
//                inserted = targetServer.insertLocalData(dataHash);
//            }
//
//            if (inserted) {
//                ExperimentRecord.allAdd++;
//                notifyNeighborServers(targetServer, dataHash);
//            }
//
//            sendKafkaEvent(timestampRel, dataHash, nodeId, operation);
//
//
//
//
//
////            try {
////                //22000log/s
////                Thread.sleep(1);
////
////            } catch (InterruptedException e) {
////                e.printStackTrace();
////            }
//
//
//
//
//        } catch (NumberFormatException e) {
//            System.err.println("[Log " + lineNumber + "] Lỗi parse số: " + line);
//        }
//    }

private boolean processLine(String line, int lineNumber) {

    MetricCollector.currentLineNumber = lineNumber;
    rateLimiter.acquire();

    String[] columns = line.trim().split("[,\\s]+");

    if (columns.length < 6) {
        System.err.println("[Log " + lineNumber + "] Bỏ qua vì không đủ cột: " + line);
        return false;
    }

    try {
        long timestampRel = System.currentTimeMillis();

        String videoId = columns[1].trim();
        long dataHash = convertVideoIdToLong(videoId);

        long nodeNumber = Long.parseLong(columns[4].trim());
        int nodeId = (int) Math.floorMod(nodeNumber, totalNodes);

        String operation = columns[5].trim().toLowerCase();

        EdgeServer targetServer = serversMap.get(nodeId);

        if (targetServer == null) {
            System.err.println("[Log " + lineNumber + "] Không tìm thấy node " + nodeId);
            return false;
        }

        // Chỉ GET mới ảnh hưởng đến cache/deduplicate/metric request.
        // Operation khác bị bỏ qua để pipeline Kafka/Spark/AI chỉ nhận GET request.
        if (!"get".equals(operation)) {
            return false;
        }

        // Tính distance trước khi insert object mới.
        // Distance này quyết định request hiện tại là Local Hit, Neighbor Hit hay Cloud Request.
        int distance = MetricCollector.measureDistance(dataHash, targetServer, serversMap);

        // Ghi nhận các metric request: Total Requests, Edge Hit, Cloud Ratio, Latency, Viral metric, Thrashing.
        MetricCollector.recordRequest(dataHash, lineNumber, distance);

        boolean existedBefore = targetServer.searchLocalData(dataHash);
        boolean inserted = false;

        if (!existedBefore) {
            // Nếu cache của node đã vượt ngưỡng storageUpperLimit, thực hiện deduplicate trước khi insert.
            // Đây là điểm EdgeServer có thể xóa object và gọi MetricCollector.recordEviction(...).
            if (targetServer.getStorageUtilization() >= this.storageUpperLimit) {
                int sizeBefore = targetServer.dataTable.size();
                double storageBefore = targetServer.getStorageUtilization();

                System.out.println(
                        "[DEDUP-BEFORE-INSERT] NODE_" + targetServer.serverID +
                                " size=" + sizeBefore +
                                ", storage=" + String.format("%.6f", storageBefore) +
                                " >= " + this.storageUpperLimit
                );

                targetServer.deduplicate(this.distancesMatrix, serversMap, new HashMap<>());

                int sizeAfter = targetServer.dataTable.size();
                double storageAfter = targetServer.getStorageUtilization();

                System.out.println(
                        "[DEDUP-AFTER-INSERT] NODE_" + targetServer.serverID +
                                " size before=" + sizeBefore +
                                ", after=" + sizeAfter +
                                ", deleted=" + (sizeBefore - sizeAfter) +
                                ", storage after=" + String.format("%.6f", storageAfter)
                );
            }

            inserted = targetServer.insertLocalData(dataHash);
        }

        if (inserted) {
            // allAdd: số object mới được insert thành công vào cache.
            // Đây là chỉ số Ripple gốc, khác với Total Requests.
            ExperimentRecord.allAdd++;

            // Cập nhật index cho các node lân cận biết object này đang nằm ở targetServer.
            notifyNeighborServers(targetServer, dataHash);
        }



        // Gửi GET log sang Kafka để Spark/AI Worker tạo feature và dự đoán viral.
        sendKafkaEvent(timestampRel, dataHash, nodeId, operation);

        return true;

    } catch (NumberFormatException e) {
        System.err.println("[Log " + lineNumber + "] Lỗi parse số: " + line);
        return false;
    }
}
    private long convertVideoIdToLong(String videoId) {
        try {
            java.security.MessageDigest digest =
                    java.security.MessageDigest.getInstance("SHA-256");

            byte[] hash = digest.digest(
                    videoId.getBytes(java.nio.charset.StandardCharsets.UTF_8)
            );

            long value = 0;
            for (int i = 0; i < 8; i++) {
                value = (value << 8) | (hash[i] & 0xff);
            }

            return Math.floorMod(value, Long.MAX_VALUE);
        } catch (Exception e) {
            throw new RuntimeException("Không thể hash videoId: " + videoId, e);
        }
    }
//    private void waitUntilLogTime(long timestampRelSeconds) {
//        long targetWallTimeMs =
//                simStartWallClockMs + (long) ((timestampRelSeconds * 1000.0) / simSpeed);
//
//        while (true) {
//            long currentMs = System.currentTimeMillis();
//
//            if (currentMs >= targetWallTimeMs) {
//                break;
//            }
//
//            long diff = targetWallTimeMs - currentMs;
//
//            try {
//                if (diff > 10) {
//                    Thread.sleep(diff);
//                }
//            } catch (InterruptedException e) {
//                Thread.currentThread().interrupt();
//                break;
//            }
//        }
//    }
    private void sendKafkaEvent(
            long timestampRel,
            long videoId,
            int nodeId,
            String operation
    ) {
        long timestampAbs = System.currentTimeMillis();

        String jsonPayload = String.format(
                java.util.Locale.US,
                "{" +
                        "\"timestamp_rel\":%d," +
                        "\"timestamp_abs\":%d," +
                        "\"key\":%d," +
                        "\"node_id\":%d," +
                        "\"operation\":\"%s\"" +
                        "}",
                timestampRel,
                timestampAbs,
                videoId,
                nodeId,
                operation
        );

        kafkaProducer.send(String.valueOf(videoId), jsonPayload);
    }

    private void notifyNeighborServers(EdgeServer targetServer, long videoId) {
        for (Map.Entry<Integer, NeighborServerInfo> entry : targetServer.neighborInfoMap.entrySet()) {
            int neighborServerId = entry.getKey();
            int hop = entry.getValue().hop;

            EdgeServer neighborServer = serversMap.get(neighborServerId);

            if (neighborServer == null) {
                continue;
            }

            UpdateMessage updateMessage =
                    new UpdateMessage("insert", targetServer.serverID, videoId, hop);

            neighborServer.updateIndex(updateMessage);
        }
    }
}