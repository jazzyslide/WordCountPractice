package com.lifexweb.app.hadoop.WordCount.ReduceSideJoinCount;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, WordValueWritable, NullWritable, WordValueWritable> {

	private WordValueWritable result = new WordValueWritable();
	
	private static Log log = LogFactory.getLog(WordCountReducer.class);
    
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
		log.info("[REDUCE] reduce task started... JobId: " + context.getJobID().getId());
	}
	
	@Override
	protected void reduce(Text word, Iterable<WordValueWritable> wordValues, Context context)
			throws IOException, InterruptedException {
		
		int count = 0;
		String tmpUrl = "";
		for (WordValueWritable value : wordValues) {
			count += value.getCount().get();
			
			if (!value.getUrl().toString().isEmpty()) {
				tmpUrl = value.getUrl().toString();
			}
		}
		result.set(word.toString(), tmpUrl, count);
		
		if (!result.getUrl().toString().isEmpty() && count > 0) {
			context.write(NullWritable.get(), result);
		}
		result.set("", "", 0);
	}
	
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		log.info("[REDUCE] reduce task finished... JobId: " + context.getJobID().getId());
		super.cleanup(context);
	}
}
