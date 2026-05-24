package edu.hust.edgededuplicate.filters;

import edu.hust.edgededuplicate.bitmap.BitMap;
import edu.hust.edgededuplicate.hash.HashFunctions;
import edu.hust.edgededuplicate.hash.HashType;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class Filter implements Comparable<Filter> {
	public int filterID;
	public int compareTo(Filter otherFilter) {
		return Integer.compare(this.filterID, otherFilter.filterID);
	}
	HashType hash_type;

	boolean rejuvenate(long key) {
		return false;
	}

	boolean expand() {
		return false;
	}

	protected boolean _delete(long large_hash) {
		return false;
	}

	protected boolean _insert(long large_hash, boolean insert_only_if_no_match) {
		return false;
	}

	protected boolean _search(long large_hash) {
		return false;
	}


	public boolean delete(long input) {
		return _delete(get_hash(input));
	}

	public boolean delete(String input) {
		ByteBuffer input_buffer = ByteBuffer.wrap(input.getBytes(StandardCharsets.UTF_8));
		return _delete(HashFunctions.xxhash(input_buffer));
	}

	public boolean delete(byte[] input) {
		ByteBuffer input_buffer = ByteBuffer.wrap(input);
		return _delete(HashFunctions.xxhash(input_buffer));
	}
	
	public boolean insert(long input, boolean insert_only_if_no_match) {		
		long hash = get_hash(input);
		return _insert(hash, insert_only_if_no_match);
	}

	public boolean insert(String input, boolean insert_only_if_no_match) {
		ByteBuffer input_buffer = ByteBuffer.wrap(input.getBytes(StandardCharsets.UTF_8));
		return _insert(HashFunctions.xxhash(input_buffer), insert_only_if_no_match);
	}

	public boolean insert(byte[] input, boolean insert_only_if_no_match) {
		ByteBuffer input_buffer = ByteBuffer.wrap(input);
		return _insert(HashFunctions.xxhash(input_buffer), insert_only_if_no_match);
	}
	
	public boolean search(long input) {
		return _search(get_hash(input));
	}

	public boolean search(String input) {
		ByteBuffer input_buffer = ByteBuffer.wrap(input.getBytes(StandardCharsets.UTF_8));
		return _search(HashFunctions.xxhash(input_buffer));
	}

	public boolean search(byte[] input) {
		ByteBuffer input_buffer = ByteBuffer.wrap(input);
		return _search(HashFunctions.xxhash(input_buffer));
	}
	
	long get_hash(long input) {
		long hash = 0;
		if (hash_type == HashType.arbitrary) {
			hash = HashFunctions.normal_hash((int)input);
		}
		else if (hash_type == HashType.xxh) {
			hash = HashFunctions.xxhash(input);
		}
		else {
			System.exit(1);
		}
		return hash;
	}

	public long get_num_entries(boolean include_all_internal_filters) {
		return 0;
	}

	public double get_utilization() {
		return 0;
	}
	
	public double measure_num_bits_per_entry() {
		return 0;
	}

	public long get_memory(){return 0;}
	
	 static void print_int_in_binary(int num, int length) {
		String str = "";
		for (int i = 0; i < length; i++) {
			int mask = (int)Math.pow(2, i);
			int masked = num & mask;
			str += masked > 0 ? "1" : "0";
		}
		System.out.println(str);
	}
	
	 static void print_long_in_binary(long num, int length) {
		String str = "";
		for (int i = 0; i < length; i++) {
			long mask = (long)Math.pow(2, i);
			long masked = num & mask;
			str += masked > 0 ? "1" : "0";
		}
		System.out.println(str);
	}
	
	String get_fingerprint_str(long fp, int length) {
		String str = "";
		for (int i = 0; i < length; i++) {
            str += BitMap.get_fingerprint_bit(i, fp) ? "1" : "0";
		}
		return str;
	}
	
	public void pretty_print() {

	}

}
