package edu.hust.edgededuplicate.filters;

public class BloomFilterExtension {
    public static CountingBloomFilter CountingBloomFilterMerge(CountingBloomFilter first,CountingBloomFilter second){
        if(first.getMax_num_entries()!=second.getMax_num_entries()||first.getBits_per_entry()!= second.getBits_per_entry()){
            throw new RuntimeException(" The two filters entered are different sizes");
        }
        CountingBloomFilter merge_filter = new CountingBloomFilter(0, first.getMax_num_entries(), first.getBits_per_entry());

        for (int i = 0; i < merge_filter.count.length; i++) {
            merge_filter.count[i] = (byte) (first.count[i] + second.count[i]);
            if(merge_filter.count[i]>0){
                merge_filter.setBit(i ,true);
            }
        }
        return merge_filter;
    }
}
