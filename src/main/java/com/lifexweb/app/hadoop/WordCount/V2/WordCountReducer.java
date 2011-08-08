package com.lifexweb.app.hadoop.WordCount.V2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.lifexweb.app.hadoop.WordCount.V2.writable.WordKeyWritable;

public class WordCountReducer extends Reducer<WordKeyWritable, IntWritable, Text, Text> {

	private Text resultKey = new Text();
	private Text resultValue = new Text();
	
	@Override
	protected void reduce(WordKeyWritable wordKey, Iterable<IntWritable> wordCounts, Context context)
			throws IOException, InterruptedException {
		
		int wordKindCount = 0;
		int wordTotalCount = 0;
		String tmpCapitalLetter = "";
		String tmpWord = "";
		for (IntWritable cnt : wordCounts) {
			if (tmpCapitalLetter.isEmpty() || tmpWord.isEmpty()) {
				//ループの最初
				tmpCapitalLetter = wordKey.getCapitLetter().toString();
				tmpWord = wordKey.getWord().toString();
				wordKindCount = 1;
				wordTotalCount = cnt.get();
			} else if (!tmpCapitalLetter.equals(wordKey.getCapitLetter().toString())){
				//前の単語と頭文字が違う場合
				resultKey.set(wordKey.getFileCode().toString() + "\t" + tmpCapitalLetter);
				resultValue.set(wordKindCount + "\t" + wordTotalCount);
				context.write(resultKey, resultValue);
				
				tmpCapitalLetter = wordKey.getCapitLetter().toString();
				wordKindCount = 1;
				wordTotalCount = cnt.get();
			} else {
				if (!tmpWord.equals(wordKey.getWord().toString())) {
					//前の単語と違う場合
					wordKindCount++;
				}
				wordTotalCount += cnt.get();
			}
		}
		if (!tmpCapitalLetter.isEmpty() && !tmpWord.isEmpty()) {
			resultKey.set(wordKey.getFileCode().toString() + "\t" + tmpCapitalLetter);
			resultValue.set(wordKindCount + "\t" + wordTotalCount);
			context.write(resultKey, resultValue);
		}
	}	
}
