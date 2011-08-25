package com.lifexweb.app.hadoop.WordCount.ReduceSideJoinCount;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;

public class WordCountCombiner extends Reducer<WordKeyWritable, WordValueWritable, WordKeyWritable, WordValueWritable> {

	private WordValueWritable resultValue = new WordValueWritable();
	
	@Override
	protected void reduce(WordKeyWritable wordKey, Iterable<WordValueWritable> wordValues, Context context)
			throws IOException, InterruptedException {
		
		int count = 0;
		for (WordValueWritable value : wordValues) {
			if (value.getCount().get() != 0) {
				count += value.getCount().get();
			}
			resultValue.set(value.getWord().toString(), value.getUrl().toString(), count);
		}
		context.write(wordKey, resultValue);
	}	
}
