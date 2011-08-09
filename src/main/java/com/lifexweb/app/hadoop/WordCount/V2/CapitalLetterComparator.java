package com.lifexweb.app.hadoop.WordCount.V2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import com.lifexweb.app.hadoop.WordCount.V2.writable.WordKeyWritable;

public class CapitalLetterComparator extends WritableComparator {

	public CapitalLetterComparator() {
		super(WordKeyWritable.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable wc1, WritableComparable wc2) {
		//単語の頭文字で比較
		WordKeyWritable wkw1 = (WordKeyWritable)wc1;
		WordKeyWritable wkw2 = (WordKeyWritable)wc2;
		return getCapitalLetter(wkw1).compareTo(getCapitalLetter(wkw2));
	}
	
	private Text getCapitalLetter(WordKeyWritable wordKey) {
		return new Text(wordKey.getWord().toString().substring(0,1));
	}

}
