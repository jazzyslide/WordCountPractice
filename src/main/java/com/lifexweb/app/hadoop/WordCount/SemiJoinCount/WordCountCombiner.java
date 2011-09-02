package com.lifexweb.app.hadoop.WordCount.SemiJoinCount;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountCombiner extends Reducer<WordKeyWritable, WordValueWritable, WordKeyWritable, WordValueWritable> {

	private WordValueWritable resultValue = new WordValueWritable();	
	private static Log log = LogFactory.getLog(WordCountCombiner.class);
    
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
		log.info("[COMBINER] combiner started... JobId: " + context.getJobID().getId());
	}
	
	@Override
	protected void reduce(WordKeyWritable wordKey, Iterable<WordValueWritable> wordValues, Context context)
			throws IOException, InterruptedException {
			
		int count = 0;
		String url = "";
		for (WordValueWritable value : wordValues) {
			url = value.getUrl().toString();
			count += value.getCount().get();
		}
		resultValue.set(wordKey.getWord().toString(), url, count);
		context.write(wordKey, resultValue);
	}
	
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		log.info("[COMBINER] combiner finished... JobId: " + context.getJobID().getId());
		super.cleanup(context);
	}
}
