package com.lifexweb.app.hadoop.WordCount.ReduceSideJoinCount;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


public class WordValueWritable implements WritableComparable<WordValueWritable> {

	private Text word;
	private Text url;
	private IntWritable count;
	
	public WordValueWritable() {
		set(new Text(), new Text(), new IntWritable());
	}
	
	public WordValueWritable(String word, String url, int count) {
		set(new Text(word), new Text(url), new IntWritable(count));
	}
	
	public WordValueWritable(Text word, Text url, IntWritable count) {
		set(word, url, count);
	}

	public void set(String word, String url, int count) {
		set(new Text(word), new Text(url), new IntWritable(count));
	}
	
	public void set(Text word, Text url, IntWritable count) {
		this.word = word;
		this.url = url;
		this.count = count;
	}
	
	public void set(WordValueWritable value) {
		this.word = value.getWord();
		this.url = value.getUrl();
		this.count = value.getCount();
	}
	

	public Text getWord() {
		return word;
	}

	public void setWord(Text word) {
		this.word = word;
	}

	public Text getUrl() {
		return url;
	}

	public void setUrl(Text url) {
		this.url = url;
	}

	public IntWritable getCount() {
		return count;
	}

	public void setCount(IntWritable count) {
		this.count = count;
	}

	public void setCount(int count) {
		this.count = new IntWritable(count);
	}
	
	public void write(DataOutput out) throws IOException {
		word.write(out);
		url.write(out);
		count.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		word.readFields(in);
		url.readFields(in);
		count.readFields(in);
	}

	public int compareTo(WordValueWritable cmpLine) {
		int cmp = word.compareTo(cmpLine.getWord());
		if (cmp != 0) {
			return cmp;
		}
		cmp = url.compareTo(cmpLine.getUrl());
		if (cmp != 0) {
			return cmp;
		}
		return count.compareTo(cmpLine.getCount());
	}
	
	@Override
	public String toString() {
		return word + "\t" + url + "\t" + count;
	}

}
