package com.lifexweb.app.hadoop.WordCount.SemiJoinCount;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class UrlFlagSortComparator extends WritableComparator {

	public UrlFlagSortComparator() {
		super(WordKeyWritable.class, true);
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable arg0, WritableComparable arg1) {
		WordKeyWritable key1 = (WordKeyWritable)arg0;
		WordKeyWritable key2 = (WordKeyWritable)arg1;
		
		int cmp = key1.getWord().compareTo(key2.getWord());
		if (cmp != 0) {
			return cmp;
		}
		
		//UrlRecortFlagが1のものからに並び替え
		return key1.getUrlRecordFlag().compareTo(key2.getUrlRecordFlag()) * -1;
	}

}
