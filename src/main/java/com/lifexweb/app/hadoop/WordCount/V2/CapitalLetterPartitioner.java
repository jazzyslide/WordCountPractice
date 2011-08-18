package com.lifexweb.app.hadoop.WordCount.V2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import com.lifexweb.app.hadoop.WordCount.V2.Writable.WordKeyWritable;

public class CapitalLetterPartitioner extends Partitioner<WordKeyWritable, IntWritable> {
		
	@Override
	public int getPartition(WordKeyWritable key, IntWritable count, int numReducer) {
		//単語の頭文字で同じ所に行くようにする
		return (key.getCapitLetter().hashCode() * 163 & Integer.MAX_VALUE) % numReducer;
	}
}
