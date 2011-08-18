package com.lifexweb.app.hadoop.WordCount.V2.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class WordKeyWritable implements WritableComparable<WordKeyWritable> {

	private Text capitLetter;
	private Text word;
	
	public WordKeyWritable() {
		set(new Text(), new Text());
	}
	
	public WordKeyWritable(int fileCode, String capitLetter, String word) {
		set(new Text(capitLetter), new Text(word));
	}
	
	public WordKeyWritable(Text capitLetter, Text word) {
		set(capitLetter, word);
	}

	public void set(String capitLetter, String word) {
		set(new Text(capitLetter), new Text(word));
	}
	
	public void set(Text capitLetter, Text word) {
		this.capitLetter = capitLetter;
		this.word = word;
	}
	
	public Text getCapitLetter() {
		return capitLetter;
	}
	
	public Text getWord() {
		return word;
	}

	@Override
	public String toString() {
		return capitLetter.toString() + "\t" + word.toString();
	}
	
	public void readFields(DataInput in) throws IOException {
		capitLetter.readFields(in);
		word.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		capitLetter.write(out);
		word.write(out);
	}
	
	public int hashCode() {
		return capitLetter.hashCode() * 163 + word.hashCode();
	}

	public int compareTo(WordKeyWritable cmpLine) {
		int cmp = capitLetter.compareTo(cmpLine.getCapitLetter());
		if (cmp != 0) {
			return cmp;
		}
		return word.compareTo(cmpLine.getWord());
	}
}
