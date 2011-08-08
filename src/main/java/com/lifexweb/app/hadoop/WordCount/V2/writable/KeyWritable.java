package com.lifexweb.app.hadoop.WordCount.V2.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class KeyWritable implements Writable {

	private IntWritable fileCode;
	private Text capitLetter;
	private Text word;
	
	public KeyWritable() {
		set(new IntWritable(), new Text(), new Text());
	}
	
	public KeyWritable(int fileCode, String capitLetter, String word) {
		set(new IntWritable(fileCode), new Text(capitLetter), new Text(word));
	}
	
	public KeyWritable(IntWritable fileCode, Text capitLetter, Text word) {
		set(fileCode, capitLetter, word);
	}

	public void set(int fileCode, String capitLetter, String word) {
		set(new IntWritable(fileCode), new Text(capitLetter), new Text(word));
	}
	
	public void set(IntWritable fileCode, Text capitLetter, Text word) {
		this.fileCode = fileCode;
		this.capitLetter = capitLetter;
		this.word = word;
	}
	
	public IntWritable getFileCode() {
		return fileCode;
	}
	
	public Text getCapitLetter() {
		return capitLetter;
	}
	
	public Text getWord() {
		return word;
	}

	@Override
	public String toString() {
		return fileCode.toString() + "\t" + capitLetter.toString() + "\t" + word.toString();
	}
	
	public void readFields(DataInput in) throws IOException {
		fileCode.readFields(in);
		capitLetter.readFields(in);
		word.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		fileCode.write(out);
		capitLetter.write(out);
		word.write(out);
	}
	
	public int hashCode() {
		return fileCode.hashCode() * 163 + capitLetter.hashCode() * 63 + word.hashCode();
	}

}
