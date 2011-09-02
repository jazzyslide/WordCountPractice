package com.lifexweb.app.hadoop.WordCount.SemiJoinCount;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<WordKeyWritable, WordValueWritable, NullWritable, WordValueWritable> {

	private WordValueWritable resultValue = new WordValueWritable();	
	private static Log log = LogFactory.getLog(WordCountReducer.class);
	private static final String URL_RECORD_FLAG_ON = "1";
    
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
		log.info("[REDUCE] reduce task started... JobId: " + context.getJobID().getId());
	}
	
	@Override
	protected void reduce(WordKeyWritable wordKey, Iterable<WordValueWritable> wordValues, Context context)
			throws IOException, InterruptedException {
			
		int count = 0;
		String url = "";
		for (WordValueWritable value : wordValues) {
			if (URL_RECORD_FLAG_ON.equals(wordKey.getUrlRecordFlag().toString())) {
				url = value.getUrl().toString();
			}
			count += value.getCount().get();
		}
		resultValue.set(wordKey.getWord().toString(), url, count);
		context.write(NullWritable.get(), resultValue);
	}
	
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		log.info("[REDUCE] reduce task finished... JobId: " + context.getJobID().getId());
		super.cleanup(context);
	}
}
