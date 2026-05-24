package edu.hust.edgededuplicate.core;

import edu.hust.edgededuplicate.core.NeighborServerInfo;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;
import java.util.Map;

public class RippleLogIngestor {

    private final Map<Integer, EdgeServer> serversMap;
    private final int nodeCount;
    private final int[][] distancesMatrix;
    private final Map<Integer, List<EdgeClient>> clientsMap;
    private final double deduplicateThreshold;

    public RippleLogIngestor(
            Map<Integer, EdgeServer> serversMap,
            int nodeCount,
            int[][] distancesMatrix,
            Map<Integer, List<EdgeClient>> clientsMap,
            double deduplicateThreshold
    ) {
        this.serversMap = serversMap;
        this.nodeCount = nodeCount;
        this.distancesMatrix = distancesMatrix;
        this.clientsMap = clientsMap;
        this.deduplicateThreshold = deduplicateThreshold;
    }

    /**
     * Đọc file log một lần từ đầu đến cuối.
     * Phù hợp khi bạn muốn test bằng file log có sẵn.
     */
    public void readFileAndSend(String requestFilePath) {
        try (BufferedReader reader = new BufferedReader(new FileReader(requestFilePath))) {
            String line;

            while ((line = reader.readLine()) != null) {
                processLine(line);
            }

        } catch (IOException e) {
            throw new RuntimeException("Không đọc được file log: " + requestFilePath, e);
        }
    }

    /**
     * Đọc realtime theo kiểu tail -f.
     * File có dòng mới thì xử lý dòng mới.
     */
    public void tailFileAndSend(String requestFilePath) {
        try (RandomAccessFile file = new RandomAccessFile(requestFilePath, "r")) {

            // Nếu muốn chỉ đọc dòng mới phát sinh sau khi chương trình chạy, dùng:
            file.seek(file.length());

            // Nếu muốn đọc từ đầu file rồi tiếp tục realtime, đổi thành:
            // file.seek(0);

            while (true) {
                String line = file.readLine();

                if (line == null) {
                    Thread.sleep(500);
                    continue;
                }

                processLine(line);
            }

        } catch (Exception e) {
            throw new RuntimeException("Lỗi khi đọc realtime file log: " + requestFilePath, e);
        }
    }

    private void processLine(String line) {
        if (line == null || line.trim().isEmpty()) {
            return;
        }

        try {
            String[] columns = line.trim().split("\\s+");

            if (columns.length < 5) {
                System.out.println("[LogIngestor] Bỏ qua dòng không đủ 5 cột: " + line);
                return;
            }

            long videoId = Long.parseLong(columns[4]);

            int nodeId = (int) Math.floorMod(videoId, nodeCount);

            EdgeServer targetServer = serversMap.get(nodeId);

            if (targetServer == null) {
                System.out.println("[LogIngestor] Không tìm thấy node: " + nodeId);
                return;
            }

            boolean inserted = targetServer.insertLocalData(videoId);

            if (!inserted) {
                return;
            }

            notifyNeighborServers(targetServer, videoId);

            System.out.println(
                    "[LogIngestor] Insert videoId=" + videoId +
                            " vào node=" + nodeId +
                            ", storage=" + targetServer.getStorageUtilization()
            );

            if (targetServer.getStorageUtilization() >= deduplicateThreshold) {
                System.out.println("[LogIngestor] Node " + nodeId + " vượt ngưỡng, bắt đầu deduplicate...");
                targetServer.deduplicate(distancesMatrix, serversMap, clientsMap);
            }

        } catch (NumberFormatException e) {
            System.out.println("[LogIngestor] Cột thứ 5 không phải số, bỏ qua dòng: " + line);
        } catch (Exception e) {
            System.out.println("[LogIngestor] Lỗi xử lý dòng: " + line);
            e.printStackTrace();
        }
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