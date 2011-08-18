package com.lifexweb.app.hadoop.WordCount.ReduceSideJoinCount;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class WordComparator extends WritableComparator {

	public WordComparator() {
		super(Text.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable wc1, WritableComparable wc2) {
		//keyとなっているwordで比較
		Text text1 = (Text)wc1;
		Text text2 = (Text)wc2;
		return text1.compareTo(text2);
	}
}
