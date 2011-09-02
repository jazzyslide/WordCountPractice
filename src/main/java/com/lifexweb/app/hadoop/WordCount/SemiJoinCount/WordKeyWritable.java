package com.lifexweb.app.hadoop.WordCount.SemiJoinCount;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


public class WordKeyWritable implements WritableComparable<WordKeyWritable> {

	private Text word;
	private Text urlRecordFlag;
	
	public WordKeyWritable() {
		set(new Text(), new Text());
	}
	
	public WordKeyWritable(String word, String urlRecordFlag) {
		set(new Text(word), new Text(urlRecordFlag));
	}
	
	public WordKeyWritable(Text word, Text urlRecordFlag) {
		set(word, urlRecordFlag);
	}

	public void set(String word, String urlRecordFlag) {
		set(new Text(word), new Text(urlRecordFlag));
	}
	
	public void set(Text word, Text urlRecordFlag) {
		this.word = word;
		this.urlRecordFlag = urlRecordFlag;
	}
	
	public void set(WordKeyWritable value) {
		this.word = value.getWord();
		this.urlRecordFlag = value.getUrlRecordFlag();
	}
	

	public Text getWord() {
		return word;
	}

	public void setWord(Text word) {
		this.word = word;
	}

	public Text getUrlRecordFlag() {
		return urlRecordFlag;
	}

	public void setUrlRecordFlag(Text urlRecordFlag) {
		this.urlRecordFlag = urlRecordFlag;
	}


	public void write(DataOutput out) throws IOException {
		word.write(out);
		urlRecordFlag.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		word.readFields(in);
		urlRecordFlag.readFields(in);
	}

	public int compareTo(WordKeyWritable cmpLine) {
		int cmp = word.compareTo(cmpLine.getWord());
		if (cmp != 0) {
			return cmp;
		}
		return urlRecordFlag.compareTo(cmpLine.getUrlRecordFlag());
	}
	
	@Override
	public String toString() {
		return word + "\t" + urlRecordFlag;
	}

}
