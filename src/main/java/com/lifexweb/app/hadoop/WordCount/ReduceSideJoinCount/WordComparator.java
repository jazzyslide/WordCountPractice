package com.lifexweb.app.hadoop.WordCount.ReduceSideJoinCount;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class WordComparator extends WritableComparator {

	public WordComparator() {
		super(WordKeyWritable.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable wc1, WritableComparable wc2) {
		//keyとなっているwordで比較
		WordKeyWritable key1 = (WordKeyWritable)wc1;
		WordKeyWritable key2 = (WordKeyWritable)wc2;
		return key1.getWord().compareTo(key2.getWord());
	}
}
