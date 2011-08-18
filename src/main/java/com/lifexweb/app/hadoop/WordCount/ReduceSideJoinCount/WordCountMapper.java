package com.lifexweb.app.hadoop.WordCount.ReduceSideJoinCount;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, WordValueWritable> {
	private Text word = new Text();
	private WordValueWritable wordValue = new WordValueWritable();
	
	private static final String PATTERN = "[-a-zA-Z0-9]+";
    private Pattern pattern = Pattern.compile(PATTERN);
    private Matcher matcher;
 
	private static Log log = LogFactory.getLog(WordCountMapper.class);

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
		log.info("[MAP:WordCountMapper] map task started... JobId: " + context.getJobID().getId());
	}
    
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();

		//正規表現でワードを抜き出すかたちに
		matcher = pattern.matcher(line);
		while (matcher.find()) {
			for (String wordStr : matcher.group().split("\\s")){
				word.set(wordStr);
				wordValue.set(wordStr, "", 1);
				context.write(word, wordValue);
			}
		}
	}
	
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		log.info("[MAP:WordCountMapper] map task finished... JobId: " + context.getJobID().getId());
		super.cleanup(context);
	}
}
