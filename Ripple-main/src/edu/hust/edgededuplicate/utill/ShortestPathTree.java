package edu.hust.edgededuplicate.utill;

import java.util.*;

public class ShortestPathTree {
    TreeMap<Integer, List<Integer>> shortestPathTree = new TreeMap<>();
    private final int[][] adjMatrix;
    private final int sourceNodeID;

    public ShortestPathTree(int[][] adjMatrix, int sourceNodeID) {
        this.adjMatrix = adjMatrix;
        this.sourceNodeID = sourceNodeID;
    }

    public TreeMap<Integer, List<Integer>> createIndexTree(int hopNum) {
        int numNodes = adjMatrix.length;
        int[] distances = new int[numNodes];
        Queue<Integer> queue = new ArrayDeque<>();

        // 初始化距离数组为-1，表示尚未访问过的节点
        Arrays.fill(distances, -1);

        // 将源节点加入队列，距离值初始化为0
        queue.add(sourceNodeID);
        distances[sourceNodeID] = 0;

        while (!queue.isEmpty()) {
            int currentNode = queue.poll();
            // 初始化当前节点的 Filter 对象作为树节点
            // 初始化节点的子节点列表
            shortestPathTree.put(currentNode, new ArrayList<>());
            // 遍历邻居节点
            for (int neighbor = 0; neighbor < numNodes; neighbor++) {
                if (adjMatrix[currentNode][neighbor] == 1 && distances[neighbor] == -1) {
                    // 更新距离值和最短路径树
                    distances[neighbor] = distances[currentNode] + 1;
                    queue.add(neighbor);
                    shortestPathTree.get(currentNode).add(neighbor);
                }
            }
        }
        return shortestPathTree;
    }
}