package com.lifexweb.app.hadoop.WordCount.ReduceSideJoinCount;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountCombiner extends Reducer<Text, WordValueWritable, Text, WordValueWritable> {

	private WordValueWritable resultValue = new WordValueWritable();
	
	@Override
	protected void reduce(Text text, Iterable<WordValueWritable> values, Context context)
			throws IOException, InterruptedException {
		
		int count = 0;
		for (WordValueWritable value : values) {
			
			//URLの行の場合はそのまま出力
			if (!value.getUrl().toString().isEmpty()) {
				context.write(text, value);
				return;
			}
			
			if (value.getCount().get() != 0) {
				count += value.getCount().get();
			}
		}
		resultValue.set(text.toString(), "", count);
		context.write(text, resultValue);
	}	
}
