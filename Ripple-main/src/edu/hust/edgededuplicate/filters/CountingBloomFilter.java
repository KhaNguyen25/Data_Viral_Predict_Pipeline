package edu.hust.edgededuplicate.filters;

public class CountingBloomFilter extends BloomFilter{

    public byte[] count;

    public CountingBloomFilter(int filterID, int new_num_entries, int new_bits_per_entry) {
        super(new_num_entries, new_bits_per_entry);
        this.filterID = filterID;
        this.count = new byte[new_num_entries*new_bits_per_entry];
    }

    public CountingBloomFilter(int new_num_entries) {
        super(new_num_entries, 32);
        this.count = new byte[new_num_entries*32];
    }
    @Override
    protected boolean _delete(long large_hash) {

        if (!_search(large_hash)){
            return false;
        }

        long target_bit = Math.abs(large_hash % num_bits);
        count[Math.toIntExact(target_bit)]--;
        if(count[Math.toIntExact(target_bit)]==0){
            filter.set(target_bit, false);
        }

        for (int i = 1; i < num_hash_functions; i++) {
            target_bit = get_target_bit(large_hash, i);
            //System.out.println(target_bit);
            count[Math.toIntExact(target_bit)]--;
            if(count[Math.toIntExact(target_bit)]==0){
                filter.set(target_bit, false);
            }
        }
        current_num_entries--;
        return true;
    }

    @Override
    protected boolean _insert(long large_hash, boolean insert_only_if_no_match) {

        long target_bit = Math.abs(large_hash % num_bits);
        filter.set(target_bit, true);
        count[Math.toIntExact(target_bit)]++;

        for (int i = 1; i < num_hash_functions; i++) {
            target_bit = get_target_bit(large_hash, i);
            //System.out.println(target_bit);
            filter.set(target_bit, true);
            count[Math.toIntExact(target_bit)]++;
        }
        current_num_entries++;
        return true;
    }

    public void setBit(long target_bit, boolean value){
        filter.set(target_bit, value);
    }

    public long get_memory(){return max_num_entries * bits_per_entry + this.count.length*Byte.SIZE;}
}
