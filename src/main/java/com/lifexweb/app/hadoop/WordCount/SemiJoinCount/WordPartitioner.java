package com.lifexweb.app.hadoop.WordCount.SemiJoinCount;

import org.apache.hadoop.mapreduce.Partitioner;

public class WordPartitioner extends Partitioner<WordKeyWritable, WordValueWritable> {

	@Override
	public int getPartition(WordKeyWritable key, WordValueWritable value, int numPartitions) {
		//keyに含まれる同一のワードでパーティション
		return (key.getWord().hashCode() * 163 & Integer.MAX_VALUE) % numPartitions;
	}

}
