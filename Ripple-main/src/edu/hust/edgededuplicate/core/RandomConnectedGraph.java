package edu.hust.edgededuplicate.core;

import edu.hust.edgededuplicate.utill.ConfigurationManager;
import edu.hust.edgededuplicate.utill.GlobalLogger;
import org.jgrapht.alg.util.UnionFind;

import java.util.*;
import java.util.logging.Logger;

public class RandomConnectedGraph {
    private static final Logger logger = GlobalLogger.getLogger();
    private final int numServers = ConfigurationManager.getIntProperty("numServers");
    private final int numEdges = ConfigurationManager.getIntProperty("numEdges");
    private final int realDataNum = ConfigurationManager.getIntProperty("realDataNum");
    private final int fakeDataNum = ConfigurationManager.getIntProperty("fakeDataNum");
    private final int numClients = ConfigurationManager.getIntProperty("numClients");
    private final int[][] adjacencyMatrix;
    public int[][] distancesMatrix;

    public int[][] getDistancesMatrix() {
        return distancesMatrix;
    }

    public int[][] getAdjacencyMatrix() {
        return adjacencyMatrix;
    }

    public int getNumServers() {
        return numServers;
    }

    public int getNumClients() {
        return numClients;
    }

//    Khởi tạo ma trận kề (adjacencyMatrix) và ma trận khoảng cách (distancesMatrix)
//    dựa trên số lượng server (numServers) được đọc từ file cấu hình.
    public RandomConnectedGraph() {
        this.adjacencyMatrix = new int[numServers][numServers];
        this.distancesMatrix = new int[numServers][numServers];
        for (int row = 0; row < numServers; row++) {
            for (int column = 0; column < numServers; column++) {
                this.adjacencyMatrix[row][column] = 0;
                this.distancesMatrix[row][column] = 0;
            }
        }
    }

//    Sử dụng thuật toán UnionFind để các máy chủ kết nối với nhau
    public void generateGraph() {
        Random rand = new Random(12345);
        // 初始化并查集
        Set<Integer> set = new HashSet<>();
        for (int nodeID = 0; nodeID < numServers; nodeID++) {
            set.add(nodeID);
        }
        UnionFind<Integer> edgeUnionFind = new UnionFind<>(set);

        // 构成基础连通图
        while (edgeUnionFind.numberOfSets() != 1) {
            int edgeNodeStart = rand.nextInt(numServers);
            int edgeNodeEnd = rand.nextInt(numServers);
            // 如果src和dest不在同一个集合，就在它们之间生成一条边
            if (!edgeUnionFind.inSameSet(edgeNodeStart, edgeNodeEnd)) {
                adjacencyMatrix[edgeNodeStart][edgeNodeEnd] = 1;
                adjacencyMatrix[edgeNodeEnd][edgeNodeStart] = 1;
                edgeUnionFind.union(edgeNodeStart, edgeNodeEnd);
            }
        }

        //随机生成numEdges + numNodes -1 条边
        for (int countEdge = 0; countEdge < numEdges - numServers + 1; countEdge++) {
            int edgeNodeStart = rand.nextInt(numServers);
            int edgeNodeEnd = rand.nextInt(numServers);
            // 如果src和dest不在同一个集合，就在它们之间生成一条边
            if (adjacencyMatrix[edgeNodeStart][edgeNodeEnd] != 1) {
                adjacencyMatrix[edgeNodeStart][edgeNodeEnd] = 1;
                adjacencyMatrix[edgeNodeEnd][edgeNodeStart] = 1;

            }
        }
        calculateServerDistances();
        logger.info("Random Connected graph creation complete: " + "Node: " + numServers + " Edge: " + numEdges);
    }

//    Sử dụng thuật toán BFS để tính toán bước nhảy giữa mỗi cặp server
    public void calculateServerDistances() {
        for (int source = 0; source < numServers; source++) {
            Queue<Integer> queue = new LinkedList<>();
            boolean[] visited = new boolean[numServers];
            int[] hopCounts = new int[numServers];

            queue.offer(source);
            visited[source] = true;

            while (!queue.isEmpty()) {
                int current = queue.poll();
                for (int neighbor = 0; neighbor < numServers; neighbor++) {
                    if (adjacencyMatrix[current][neighbor] == 1 && !visited[neighbor]) {
                        visited[neighbor] = true;
                        hopCounts[neighbor] = hopCounts[current] + 1;
                        distancesMatrix[source][neighbor] = hopCounts[neighbor];
                        queue.offer(neighbor);
                    }
                }
            }
        }
    }

//    Tạo ra 1 số nguyên ngẫu nhiên đại diện chi ID của 1 video thật
    public long generateRealData() {
        Random random = new Random();
        return Math.abs(random.nextLong()) % realDataNum;
    }

//    Tạo ra danh sách các video giả
    public List<Long> generateFakeDataArray() {
        List<Long> fakeDataList = new ArrayList<>();
        for (int i = 0; i < fakeDataNum; i++) {
            Random random = new Random();
            long data = Math.abs(random.nextLong()) % realDataNum + realDataNum;
            fakeDataList.add(data);
        }
        return fakeDataList;
    }

//    Chọn ngẫu nhiên một ID máy chủ để thực hiện
//    các thao tác như gửi yêu cầu hoặc đặt dữ liệu.
    public int generateRandomServerID() {
        Random random = new Random();
        return random.nextInt(numServers);
    }

    public double generateRandomDouble() {
        Random random = new Random();
        return random.nextDouble();
    }

//    mô phỏng độ hot của video hoặc dung lượng bộ nhớ của các server biên
    public double generateNormalDistributionRandomDouble() {
        Random random = new Random();

        double u1 = random.nextDouble(); // 生成0到1之间的随机数
        double u2 = random.nextDouble();

        // 使用Box-Muller转换生成正态分布随机数
        double z0 = Math.sqrt(-2 * Math.log(u1)) * Math.cos(2 * Math.PI * u2);

        // 将z0缩放到均值为0，标准差为1的正态分布，如果需要在0到1之间，可以使用z0的CDF
        double mean = 0.5; // 均值
        double stdDev = 0.15; // 标准差
        double normalRandom = mean + stdDev * z0;

        // 确保结果在0到1之间
        if (normalRandom < 0) {
            normalRandom = 0;
        } else if (normalRandom > 1) {
            normalRandom = 1;
        }
        return normalRandom;
    }
}
