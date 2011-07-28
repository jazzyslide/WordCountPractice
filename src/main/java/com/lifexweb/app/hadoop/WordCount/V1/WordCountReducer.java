package com.lifexweb.app.hadoop.WordCount.V1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	private IntWritable countResult = new IntWritable(0);
	
	@Override
	protected void reduce(Text word, Iterable<IntWritable> counts, Context context)
			throws IOException, InterruptedException {
		
		int count = 0;
		for (IntWritable c : counts) {
			count += c.get();
		}
		
		countResult.set(count);
		context.write(word, countResult);
	
	}
	
	
}
