package com.lifexweb.app.hadoop.WordCount.ReduceSideJoinCount;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SiteUrlMapper extends Mapper<LongWritable, Text, Text, WordValueWritable> {
	private Text wordKey = new Text();
	private WordValueWritable wordValue = new WordValueWritable();
	
	private static Log log = LogFactory.getLog(SiteUrlMapper.class);

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
		log.info("[MAP:SiteUrlMapper] map task started... JobId: " + context.getJobID().getId());
	}
    
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] wordUrlPair = line.split("\\t");
		
		//正しいデータ数（ペア）かチェックする
		if (wordUrlPair.length != 2) {
			log.warn("[MAP:SiteUrlMapper] SKIP : Invalid Num of Elements:["+ context.getCurrentValue() + "]");
			return;
		}
		wordValue.set(wordUrlPair[0], wordUrlPair[1], 0);
		wordKey.set(wordUrlPair[0]);
		context.write(wordKey, wordValue);
	}
	
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		log.info("[MAP:SiteUrlMapper] map task finished... JobId: " + context.getJobID().getId());
		super.cleanup(context);
	}
}
