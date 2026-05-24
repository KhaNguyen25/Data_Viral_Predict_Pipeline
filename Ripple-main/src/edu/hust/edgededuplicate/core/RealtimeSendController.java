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
    private final RateLimiter rateLimiter = RateLimiter.create(21000);

    private final int maxTestLines;
    private final int dedupBatchSize;

    public RealtimeSendController(Map<Integer, EdgeServer> serversMap, int totalNodes, int[][] distancesMatrix) {
        this.serversMap = serversMap;
        this.totalNodes = totalNodes;
        this.kafkaProducer = new KafkaRequestProducer();

        this.distancesMatrix = distancesMatrix;
        this.storageUpperLimit = ConfigurationManager.getDoubleProperty("storageUpperLimit");
        this.simSpeed = ConfigurationManager.getDoubleProperty("simSpeed");

        this.maxTestLines = ConfigurationManager.getIntProperty("maxTestLines");
        this.dedupBatchSize = ConfigurationManager.getIntProperty("dedupBatchSize");
    }

//    public void startStreaming(String filePath) {
//
//        System.out.println("Bắt đầu nạp dữ liệu từ file log: " + filePath);
//        this.simStartWallClockMs = System.currentTimeMillis();
//        try (BufferedReader br = new BufferedReader(
//                new InputStreamReader(new GZIPInputStream(new FileInputStream(filePath))))) {
//            String line;
//            int lineNumber = 0;
//
//
//            long startMs = System.currentTimeMillis();
//            while ((line = br.readLine()) != null) {
//                lineNumber++;
//
//                if (line.trim().isEmpty()) {
//                    continue;
//                }
//
//                // Nếu dòng đầu là header thật, bỏ qua.
//                // Ví dụ: uid timestamp lon lat videoId
//                if (lineNumber == 1 && !Character.isDigit(line.trim().charAt(0))) {
//                    continue;
//                }
//
//                processLine(line, lineNumber);
//                if (lineNumber % 10_000 == 0) {
//                    long elapsedMs = System.currentTimeMillis() - startMs;
//                    double seconds = elapsedMs / 1000.0;
//                    double linesPerSecond = lineNumber / seconds;
//
//                    System.out.println(
//                            "[SPEED] line=" + lineNumber +
//                                    ", elapsed=" + String.format("%.2f", seconds) + "s" +
//                                    ", speed=" + String.format("%.2f", linesPerSecond) + " lines/s"
//                    );
//                }
//                if (lineNumber >= 300000) {
//                    System.out.println("Đã chạy đủ 100000 dòng test, dừng streaming.");
//                    break;
//                }
//
//            }
//
//            System.out.println("Đã nạp xong toàn bộ file log!");
//
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        finally {
//            kafkaProducer.close();
//        }
//    }

    public void startStreaming(String filePath) {

        System.out.println("Bắt đầu nạp dữ liệu từ file log: " + filePath);
        this.simStartWallClockMs = System.currentTimeMillis();

        boolean stoppedByLimit = false;
        long startMs = System.currentTimeMillis();

        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(new GZIPInputStream(new FileInputStream(filePath))))) {

            String line;
            int lineNumber = 0;

            boolean alreadyDedupAtThisLine = false;
            while ((line = br.readLine()) != null) {
                lineNumber++;

                if (line.trim().isEmpty()) {
                    continue;
                }

                if (lineNumber == 1 && !Character.isDigit(line.trim().charAt(0))) {
                    continue;
                }

                processLine(line, lineNumber);

                alreadyDedupAtThisLine = false;

                if (dedupBatchSize > 0 && lineNumber % dedupBatchSize == 0) {
                    runBatchDedup(lineNumber);
                    alreadyDedupAtThisLine = true;
                }

                if (lineNumber % 10_000 == 0) {
                    long elapsedMs = System.currentTimeMillis() - startMs;
                    double seconds = elapsedMs / 1000.0;
                    double speed = lineNumber / Math.max(seconds, 0.001);

                    System.out.println(
                            "[SPEED] line=" + lineNumber +
                                    ", elapsed=" + String.format("%.2f", seconds) + "s" +
                                    ", speed=" + String.format("%.2f", speed) + " lines/s"
                    );
                }

                if (maxTestLines > 0 && lineNumber >= maxTestLines) {
                    System.out.println("Đã chạy đủ " + maxTestLines + " dòng test, dừng streaming.");
                    stoppedByLimit = true;
                    break;
                }
            }

            // Chạy dedup thêm một lần cuối sau khi dừng batch
            if (!alreadyDedupAtThisLine) {
                runBatchDedup(lineNumber);
            }

            if (stoppedByLimit) {
                System.out.println("Đã dừng theo giới hạn test, chưa nạp hết toàn bộ file log.");
            } else {
                System.out.println("Đã nạp xong toàn bộ file log!");
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

    private void processLine(String line, int lineNumber) {
        rateLimiter.acquire();
        // Dùng được cả log cách nhau bằng khoảng trắng hoặc dấu phẩy
        String[] columns = line.trim().split("[,\\s]+");

        if (columns.length < 6) {
            System.err.println("[Log " + lineNumber + "] Bỏ qua vì không đủ cột: " + line);
            return;
        }

        try {
            /*
             * Theo mô tả của bạn:
             * - Cột thứ 5, index 4, là giá trị dùng để chia dư cho 30.
             * - Giá trị đó cũng nên được dùng làm videoId/dataHash trong Ripple.
             */
            long timestampRel = System.currentTimeMillis();

            String videoId = columns[1];
            long dataHash = convertVideoIdToLong(videoId);

            long nodeNumber = Long.parseLong(columns[4]);
            int nodeId = (int) Math.floorMod(nodeNumber, totalNodes);


            String operation = columns[5];



//            waitUntilLogTime(timestampRel);

            EdgeServer targetServer = serversMap.get(nodeId);

            if (targetServer == null) {
                System.err.println("[Log " + lineNumber + "] Không tìm thấy node " + nodeId);
                return;
            }

            boolean existedBefore = targetServer.searchLocalData(dataHash);
            boolean inserted = false;

//            if (!existedBefore && targetServer.getStorageUtilization() >= this.storageUpperLimit) {
//                int sizeBefore = targetServer.dataTable.size();
//                double storageBefore = targetServer.getStorageUtilization();
//
//                System.out.println(
//                        "[DEDUP-BEFORE-INSERT] NODE_" + (nodeId+1) +
//                                " storage=" + String.format("%.4f", storageBefore) +
//                                " >= " + this.storageUpperLimit +
//                                ", start deduplicate..."
//                );
//
//                targetServer.deduplicate(this.distancesMatrix, serversMap, new HashMap<>());
//
//                int sizeAfter = targetServer.dataTable.size();
//                double storageAfter = targetServer.getStorageUtilization();
//                int deleted = sizeBefore - sizeAfter;
//
//                System.out.println(
//                        "[DEDUP-DONE] NODE_" + (nodeId+1) +
//                                " size before=" + sizeBefore +
//                                ", after=" + sizeAfter +
//                                ", deleted=" + deleted +
//                                ", storage before=" + String.format("%.6f", storageBefore) +
//                                ", after=" + String.format("%.6f", storageAfter)
//                );
//            }





            if (!existedBefore) {
                inserted = targetServer.insertLocalData(dataHash);
            }

            if (inserted) {
                ExperimentRecord.allAdd++;
                notifyNeighborServers(targetServer, dataHash);
            }

            sendKafkaEvent(timestampRel, dataHash, nodeId, operation);





//            try {
//                //22000log/s
//                Thread.sleep(1);
//
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }




        } catch (NumberFormatException e) {
            System.err.println("[Log " + lineNumber + "] Lỗi parse số: " + line);
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