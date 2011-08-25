package com.lifexweb.app.hadoop.WordCount.ReduceSideJoinCount;

import org.apache.hadoop.mapreduce.Partitioner;

public class WordPartitioner extends Partitioner<WordKeyWritable, WordValueWritable> {
	
	@Override
	public int getPartition(WordKeyWritable wordKey, WordValueWritable wordValue, int numReducer) {
		//同じwordで同じ所に行くようにする
		return (wordKey.getWord().hashCode() * 163 & Integer.MAX_VALUE) % numReducer;
	}
}
