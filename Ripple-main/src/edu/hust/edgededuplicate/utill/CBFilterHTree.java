package edu.hust.edgededuplicate.utill;

import edu.hust.edgededuplicate.filters.CountingBloomFilter;
import edu.hust.edgededuplicate.filters.Tests;

import java.util.*;

public class CBFilterHTree extends HierarchicalTree {
    private final int new_num_entries = ConfigurationManager.getIntProperty("new_num_entries");
    private static final double fprLimit = ConfigurationManager.getDoubleProperty("CBFFPRLimit");
    private CountingBloomFilter rootFilter;

    private int bits_per_entry;

    private long allMemory;
    private TreeMap<CountingBloomFilter, List<CountingBloomFilter>> hierarchicalTree = new TreeMap<>();

    public CBFilterHTree(int[][] adjMatrix, int sourceNodeID) {
        super(adjMatrix, sourceNodeID);
        bits_per_entry = (int) Math.ceil(-Math.log(fprLimit)/(Math.log(2)*Math.log(2)));
        this.hierarchicalTree = createIndexTree();
    }

    public TreeMap<CountingBloomFilter, List<CountingBloomFilter>> createIndexTree() {
        int currentFilterNum = this.filterNum;
        int level = 1;
        // 每一层的新节点
        List<CountingBloomFilter> rootFilterList = new ArrayList<>();
        // 遍历每一层
        while (level <= hopNum) {
            // 获取对应层的Filter
            List<CountingBloomFilter> filterAtLevel = getFiltersAtLevel(level);
            if (filterAtLevel.isEmpty()) {
                break;
            }
            // 构造新的节点
            CountingBloomFilter filter = new CountingBloomFilter(currentFilterNum++, new_num_entries, bits_per_entry);
            rootFilterList.add(filter);
            hierarchicalTree.put(filter, filterAtLevel);
            level++;
        }
        rootFilter = new CountingBloomFilter(currentFilterNum, new_num_entries, bits_per_entry);
        hierarchicalTree.put(rootFilter, rootFilterList);
        return hierarchicalTree;
    }

    public List<CountingBloomFilter> getFiltersAtLevel(int level) {
        List<CountingBloomFilter> result = new ArrayList<>();
        int currentLevel = 0;
        Queue<Integer> queue = new LinkedList<>();
        queue.offer(this.sourceFilterID);
        while (!queue.isEmpty() && currentLevel <= level) {
            int levelSize = queue.size();
            for (int i = 0; i < levelSize; i++) {
                int filterID = queue.poll();
                if (currentLevel == level) {
                    CountingBloomFilter filter = new CountingBloomFilter(filterID, new_num_entries, bits_per_entry);
                    result.add(filter);
                }
                List<Integer> children = this.SPTree.get(filterID);
                if (children != null) {
                    queue.addAll(children);
                }
            }
            currentLevel++;
        }
        return result;
    }

    /**
     * find the path from root node to the target node in the index tree
     * @param serverID serverID
     * @param hop the server hop
     * @return the path from root node to the target
     */
    public List<CountingBloomFilter> findPath(int serverID, int hop){

        List<CountingBloomFilter> result = new ArrayList<>();
        CountingBloomFilter levelFilter = hierarchicalTree.get(rootFilter).get(hop - 1);
        List<CountingBloomFilter> filtersAtLevel = hierarchicalTree.get(levelFilter);
        //if server exist, add path to result
        if (filtersAtLevel.stream().anyMatch(e -> e.filterID == serverID)) {
            result.add(rootFilter);
            result.add(levelFilter);
            result.add(filtersAtLevel.stream().filter(e -> e.filterID == serverID).findFirst().get());
        }
        return result;
    }

    /**
     * insert data
     * @param hash data hash
     * @param serverID which server to add data
     * @param hop the server hop
     * @return boolean
     */
    public boolean insert(long hash, int serverID, int hop) {
        List<CountingBloomFilter> path = findPath(serverID,hop);
        if(path.isEmpty()){
            logger.warning("insert server:" + serverID + ", but server not exist");
            return false;
        }
        for (CountingBloomFilter countingBloomFilter : path) {
            countingBloomFilter.insert(hash,false);
        }
        return true;
    }

    /**
     * delete a data in index tree
     * @param hash data hash
     * @param serverID which server to add data
     * @param hop the server hop
     * @return boolean
     */
    public boolean delete(long hash, int serverID, int hop) {
        List<CountingBloomFilter> path = findPath(serverID,hop);
        if(path.isEmpty()){//server path not exist
            logger.warning("insert server:" + serverID + ", but server not exist");
            return false;
        }
        if(!path.get(path.size() - 1).search(hash)){ //target server dont have this data
            return false;
        }
        for (CountingBloomFilter countingBloomFilter : path) {
            countingBloomFilter.delete(hash);
        }
        return true;
    }

    public boolean searchRoot(long hash) {
        return rootFilter.search(hash);
    }

    /**
     * query the level node of hop
     * @param hash data hash
     * @param hop the server hop
     * @return boolean
     */
    public boolean searchHop(long hash, int hop) {
        if (hop > hopNum) return false;
        if(!rootFilter.search(hash)){
            return false;
        }
        return hierarchicalTree.get(rootFilter).get(hop - 1).search(hash);
    }

    /**
     * query the level node of hop
     * @param hash data hash
     * @param hop the server hop
     * @return Server list of hop with data hash
     */
    public List<Integer> searchOnlyHopReturnID(long hash, int hop) {
        List<Integer> result = new ArrayList<>();
        if (searchHop(hash, hop)) {
            CountingBloomFilter levelFilter = hierarchicalTree.get(rootFilter).get(hop-1); //find the level node
            List<CountingBloomFilter> filters =  hierarchicalTree.get(levelFilter).stream().filter(e->e.search(hash)).toList(); // find filters with data
            for (CountingBloomFilter filter : filters) {
                result.add(filter.filterID);
            }
        }
        return result;
    }

    /**
     * query the all node
     * @param hash data hash
     * @return Server list of hop with data hash
     */
    public List<Integer> searchAllReturnID(long hash) {
        List<Integer> result = new ArrayList<>();
        if(rootFilter.search(hash)){
            List<CountingBloomFilter> levelFilters = hierarchicalTree.get(rootFilter).stream().filter(e->e.search(hash)).toList();
            for (CountingBloomFilter levelFilter : levelFilters) {
                List<CountingBloomFilter> filters =  hierarchicalTree.get(levelFilter).stream().filter(e->e.search(hash)).toList();
                for (CountingBloomFilter filter : filters) {
                    result.add(filter.filterID);
                }
            }
        }
        return result;
    }

    public double testRootFPR(List<Long> testData){
        return Tests.testFalsePositiveRate(rootFilter,testData);
    }

    public double testLevelFPR(List<Long> testData){
        double result=0;
        List<CountingBloomFilter> testFilters;
        testFilters = hierarchicalTree.get(rootFilter);
        for (CountingBloomFilter testFilter : testFilters) {
            result += Tests.testFalsePositiveRate(testFilter, testData);
        }
        result = (result + testRootFPR(testData))/(testFilters.size()+1);
        return result;
    }

    public double testAllFPR(List<Long> testData){
        double result=0;
        List<CountingBloomFilter> testFilters = new ArrayList<>();
        List<CountingBloomFilter> levelFilters = hierarchicalTree.get(rootFilter);
        for (CountingBloomFilter levelFilter : levelFilters) {
            testFilters.addAll(hierarchicalTree.get(levelFilter));
        }
        for (CountingBloomFilter testFilter : testFilters) {
            result += Tests.testFalsePositiveRate(testFilter,testData);
        }
        result = result/testFilters.size();
        result = (result + testRootFPR(testData) +testLevelFPR(testData))/3;
        return result;
    }

    public long getAllMemory(){
        long result = 0;
        result += rootFilter.get_memory();
        List<CountingBloomFilter> levelFilterList = hierarchicalTree.get(rootFilter);
        for (CountingBloomFilter countingBloomFilter : levelFilterList) {
            result+= countingBloomFilter.get_memory();
            result += hierarchicalTree.get(countingBloomFilter).stream().mapToLong(CountingBloomFilter::get_memory).sum();
        }
        return  result;
    }
}
