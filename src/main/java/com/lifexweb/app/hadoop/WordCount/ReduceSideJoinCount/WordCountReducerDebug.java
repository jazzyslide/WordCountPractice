package com.lifexweb.app.hadoop.WordCount.ReduceSideJoinCount;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducerDebug extends Reducer<Text, WordValueWritable, NullWritable, WordValueWritable> {

	private static Log log = LogFactory.getLog(WordCountReducerDebug.class);
    
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
		log.info("[REDUCE] reduce task started... JobId: " + context.getJobID().getId());
	}
	
	@Override
	protected void reduce(Text word, Iterable<WordValueWritable> wordValues, Context context)
			throws IOException, InterruptedException {
		
		for (WordValueWritable value : wordValues) {
			context.write(NullWritable.get(), value);
		}
		context.write(NullWritable.get(), new WordValueWritable("------", "-------", 0));
	}
	
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		log.info("[REDUCE] reduce task finished... JobId: " + context.getJobID().getId());
		super.cleanup(context);
	}
}
