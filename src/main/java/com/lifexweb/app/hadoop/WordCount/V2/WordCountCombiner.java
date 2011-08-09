package com.lifexweb.app.hadoop.WordCount.V2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import com.lifexweb.app.hadoop.WordCount.V2.writable.WordKeyWritable;

public class WordCountCombiner extends Reducer<WordKeyWritable, IntWritable, WordKeyWritable, IntWritable> {

	private WordKeyWritable resultKey = new WordKeyWritable();
	private IntWritable resultValue = new IntWritable();
	
	@Override
	protected void reduce(WordKeyWritable wordKey, Iterable<IntWritable> wordCounts, Context context)
			throws IOException, InterruptedException {
		
		int wordTotalCount = 0;
		String tmpWord = "";
		for (IntWritable cnt : wordCounts) {
			if (tmpWord.isEmpty()) {
				//ループの最初
				tmpWord = wordKey.getWord().toString();
				wordTotalCount = cnt.get();
			} else if (!tmpWord.equals(wordKey.getWord().toString())){
				//前の単語と違う場合
				resultKey.set(wordKey.getCapitLetter().toString(), tmpWord);
				resultValue.set(wordTotalCount);
				context.write(resultKey, resultValue);
				
				tmpWord = wordKey.getWord().toString();
				wordTotalCount = cnt.get();
			} else {
				wordTotalCount += cnt.get();
			}
		}
		if (!tmpWord.isEmpty()) {
			resultKey.set(wordKey.getCapitLetter().toString(), tmpWord);
			resultValue.set(wordTotalCount);
			context.write(resultKey, resultValue);
		}
	}	
}
