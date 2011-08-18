package com.lifexweb.app.hadoop.WordCount.ReduceSideJoinCount;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class WordPartitioner extends Partitioner<Text, WordValueWritable> {
	
	@Override
	public int getPartition(Text word, WordValueWritable wordValue, int numReducer) {
		//同じwordで同じ所に行くようにする
		return (word.hashCode() * 163 & Integer.MAX_VALUE) % numReducer;
	}
}
