package com.lifexweb.app.hadoop.WordCount.ReduceSideJoinCount;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class WordSortComparator extends WritableComparator {

	public WordSortComparator() {
		super(WordKeyWritable.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable wc1, WritableComparable wc2) {
		//keyとなっているwordで比較したあと、Flagの降順
		WordKeyWritable key1 = (WordKeyWritable)wc1;
		WordKeyWritable key2 = (WordKeyWritable)wc2;
		int cmp = key1.getWord().compareTo(key2.getWord());
		if (cmp != 0) {
			return cmp;
		}
		return key1.getUrlRecordFlag().compareTo(key2.getUrlRecordFlag()) * -1;
	}
}
