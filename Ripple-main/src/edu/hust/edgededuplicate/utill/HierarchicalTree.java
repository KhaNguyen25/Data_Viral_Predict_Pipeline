package edu.hust.edgededuplicate.utill;

import edu.hust.edgededuplicate.filters.CountingBloomFilter;

import java.util.List;
import java.util.Stack;
import java.util.TreeMap;
import java.util.logging.Logger;

public class HierarchicalTree {
    protected static final Logger logger = GlobalLogger.getLogger();
    protected final int hopNum = ConfigurationManager.getIntProperty("hopNum");
    protected TreeMap<Integer, List<Integer>> SPTree;
    protected int sourceFilterID;
    protected int filterNum;

    public HierarchicalTree(int[][] adjMatrix, int sourceFilterID) {
        this.sourceFilterID = sourceFilterID;
        this.filterNum = adjMatrix.length;
        this.SPTree = new ShortestPathTree(adjMatrix, sourceFilterID).createIndexTree(hopNum);
    }

    public boolean insert(long hash, int serverID, int hop) {
        Stack<CountingBloomFilter> path;
        return false;
    }

    public boolean delete(long hash, int serverID, int hop) {
        return  false;
    }

    public boolean searchRoot(long hash) {
        return false;
    }

    public boolean searchHop(long hash, int hop) {
        return false;
    }

    public List<Integer> searchOnlyHopReturnID(long hash, int hop) {
        return null;
    }

    public List<Integer> searchAllReturnID(long hash) {
        return null;
    }

    public double testRootFPR(List<Long> testData){
        return 0;
    }

    public double testLevelFPR(List<Long> testData){
        return 0;
    }

    public double testAllFPR(List<Long> testData){
        return 0;
    }

    public long getAllMemory(){return 0;}
}
