package edu.hust.edgededuplicate.utill;

import edu.hust.edgededuplicate.filters.AutoExpandQuotientFilter;
import edu.hust.edgededuplicate.filters.QuotientFilter;
import edu.hust.edgededuplicate.filters.Tests;

import java.util.*;

public class AEQFilterHTree extends HierarchicalTree {
    private static final int power_of_two = ConfigurationManager.getIntProperty("power_of_two");
    private static final double fprLimit = ConfigurationManager.getDoubleProperty("AEQFPRLimit");

    private Map<Integer,Integer> serversVolume;
    private AutoExpandQuotientFilter rootFilter;
    private TreeMap<AutoExpandQuotientFilter, List<AutoExpandQuotientFilter>> hierarchicalTree = new TreeMap<>();


    public AEQFilterHTree(int[][] adjMatrix, int sourceNodeID, Map<Integer,Integer> serversVolume) {
        super(adjMatrix, sourceNodeID);
        this.serversVolume = serversVolume;
        hierarchicalTree=createIndexTree();
    }

    public TreeMap<AutoExpandQuotientFilter, List<AutoExpandQuotientFilter>> createIndexTree() {
        int currentFilterNum = this.filterNum;
        int level = 1;
        // 每一层的新节点
        List<AutoExpandQuotientFilter> rootFilterList = new ArrayList<>();
        int totalVolume = 0;
        // 遍历每一层
        while (level <= hopNum) {
            // 获取对应层的Filter
            List<AutoExpandQuotientFilter> filterAtLevel = getFiltersAtLevel(level);
            if (filterAtLevel.isEmpty()) {
                break;
            }
            int volume = 0;
            for (AutoExpandQuotientFilter autoExpandQuotientFilter : filterAtLevel) {
                volume+= serversVolume.get(autoExpandQuotientFilter.filterID);
            }
            totalVolume += volume;
            int hashLength = (int) Math.ceil((Math.log(volume)-Math.log(fprLimit))/Math.log(2));
            // 构造新的节点
            AutoExpandQuotientFilter filter = new AutoExpandQuotientFilter(currentFilterNum++, power_of_two,hashLength-power_of_two+3);
            rootFilterList.add(filter);
            hierarchicalTree.put(filter, filterAtLevel);
            level++;
        }
        int hashLength = (int) Math.ceil((Math.log(totalVolume)-Math.log(fprLimit))/Math.log(2));
        rootFilter = new AutoExpandQuotientFilter(currentFilterNum, power_of_two, hashLength - power_of_two+3);
        hierarchicalTree.put(rootFilter, rootFilterList);
        return hierarchicalTree;
    }

    public List<AutoExpandQuotientFilter> getFiltersAtLevel(int level) {
        List<AutoExpandQuotientFilter> result = new ArrayList<>();
        int currentLevel = 0;
        Queue<Integer> queue = new LinkedList<>();
        queue.offer(this.sourceFilterID);
        while (!queue.isEmpty() && currentLevel <= level) {
            int levelSize = queue.size();
            for (int i = 0; i < levelSize; i++) {
                Integer filterID = queue.poll();
                if (currentLevel == level) {
                    int volume = serversVolume.get(filterID);
                    int hashLength = (int) Math.ceil((Math.log(volume)-Math.log(fprLimit))/Math.log(2));
                    AutoExpandQuotientFilter filter = new AutoExpandQuotientFilter(filterID, power_of_two,hashLength-power_of_two+3);
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
     * @param serverID
     * @param hop
     * @return the path from root node to the target
     */
    public List<AutoExpandQuotientFilter> findPath(int serverID, int hop){
        List<AutoExpandQuotientFilter> result = new ArrayList<>();
        AutoExpandQuotientFilter levelFilter = hierarchicalTree.get(rootFilter).get(hop - 1);
        List<AutoExpandQuotientFilter> filtersAtLevel = hierarchicalTree.get(levelFilter);
        //if server exist, add path to result
        if (filtersAtLevel.stream().anyMatch(e -> e.filterID == serverID)) {
            result.add(rootFilter);
            result.add(levelFilter);
            result.add(filtersAtLevel.stream().filter(e -> e.filterID == serverID).findFirst().get());
        }
        return result;
    }

    /**
     *
     * @param hash
     * @param serverID
     * @param hop
     * @return
     */
    public boolean insert(long hash, int serverID, int hop) {
        List<AutoExpandQuotientFilter> path = findPath(serverID,hop);
        if(path.isEmpty()){
            logger.warning("insert server:" + serverID + ", but server not exist");
            return false;
        }
        for (AutoExpandQuotientFilter AutoExpandQuotientFilter : path) {
            AutoExpandQuotientFilter.insert(hash,false);
        }
        return true;
    }

    /**
     * delete a data in index tree
     * @param hash
     * @param serverID
     * @param hop
     * @return
     */
    public boolean delete(long hash, int serverID, int hop) {
        List<AutoExpandQuotientFilter> path = findPath(serverID,hop);
        if(path.isEmpty()){//server path not exist
            logger.warning("insert server:" + serverID + ", but server not exist");
            return false;
        }
        if(!path.get(path.size() - 1).search(hash)){ //target server dont have this data
            return false;
        }
        for (AutoExpandQuotientFilter AutoExpandQuotientFilter : path) {
            AutoExpandQuotientFilter.delete(hash);
        }
        return true;
    }

    public boolean searchRoot(long hash) {
        return rootFilter.search(hash);
    }

    /**
     *
     * @param hash
     * @param hop
     * @return
     */
    public boolean searchHop(long hash, int hop) {
        if (hop > hopNum) return false;
        if(!rootFilter.search(hash)){
            return false;
        }
        return hierarchicalTree.get(rootFilter).get(hop - 1).search(hash);
    }

    /**
     *
     * @param hash
     * @param hop
     * @return Server list of hop with data hash
     */
    public List<Integer> searchOnlyHopReturnID(long hash, int hop) {
        List<Integer> result = new ArrayList<>();
        if (searchHop(hash, hop)) {
            AutoExpandQuotientFilter levelFilter = hierarchicalTree.get(rootFilter).get(hop-1); //find the level node
            List<AutoExpandQuotientFilter> filters =  hierarchicalTree.get(levelFilter).stream().filter(e->e.search(hash)).toList(); // find filters with data
            for (AutoExpandQuotientFilter filter : filters) {
                result.add(filter.filterID);
            }
        }
        return result;
    }

    /**
     *
     * @param hash
     * @return
     */
    public List<Integer> searchAllReturnID(long hash) {
        List<Integer> result = new ArrayList<>();
        if(rootFilter.search(hash)){
            List<AutoExpandQuotientFilter> levelFilters = hierarchicalTree.get(rootFilter).stream().filter(e->e.search(hash)).toList();
            for (AutoExpandQuotientFilter levelFilter : levelFilters) {
                List<AutoExpandQuotientFilter> filters =  hierarchicalTree.get(levelFilter).stream().filter(e->e.search(hash)).toList();
                for (AutoExpandQuotientFilter filter : filters) {
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
        List<AutoExpandQuotientFilter> testFilters;
        testFilters = hierarchicalTree.get(rootFilter);
        for (AutoExpandQuotientFilter testFilter : testFilters) {
            result += Tests.testFalsePositiveRate(testFilter, testData);
        }
        result = (result + testRootFPR(testData))/(testFilters.size()+1);
        return result;
    }

    public double testAllFPR(List<Long> testData){
        double result=0;
        List<AutoExpandQuotientFilter> testFilters = new ArrayList<>();
        List<AutoExpandQuotientFilter> levelFilters = hierarchicalTree.get(rootFilter);
        for (AutoExpandQuotientFilter levelFilter : levelFilters) {
            testFilters.addAll(hierarchicalTree.get(levelFilter));
        }
        for (AutoExpandQuotientFilter testFilter : testFilters) {
            result += Tests.testFalsePositiveRate(testFilter,testData);
        }
        result = result/testFilters.size();
        result = (result + testRootFPR(testData) +testLevelFPR(testData))/3;
        return result;
    }

    public long getAllMemory(){
        long result = 0;
        result += rootFilter.get_memory();
        List<AutoExpandQuotientFilter> levelFilterList = hierarchicalTree.get(rootFilter);
        for (AutoExpandQuotientFilter autoExpandQuotientFilter : levelFilterList) {
            result+= autoExpandQuotientFilter.get_memory();
            result += hierarchicalTree.get(autoExpandQuotientFilter).stream().mapToLong(QuotientFilter::get_memory).sum();
        }
        return  result;
    }
}
