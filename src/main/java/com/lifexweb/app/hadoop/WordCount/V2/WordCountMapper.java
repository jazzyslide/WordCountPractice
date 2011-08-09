package com.lifexweb.app.hadoop.WordCount.V2;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.lifexweb.app.hadoop.WordCount.V2.writable.WordKeyWritable;

public class WordCountMapper extends Mapper<LongWritable, Text, WordKeyWritable, IntWritable> {
	private static final IntWritable ONE = new IntWritable(1);
	private Text word = new Text();
	private WordKeyWritable wordKey = new WordKeyWritable();
	
	private static final String PATTERN = "[a-zA-Z0-9]+";
    private Pattern pattern = Pattern.compile(PATTERN);
    private Matcher matcher;
    
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		line = line.toLowerCase();

		//正規表現でワードを抜き出すかたちに
		matcher = pattern.matcher(line);
		while (matcher.find()) {
			for (String wordStr : matcher.group().split("\\s")){
				//ファイル識別子　＋　単語の頭文字　＋　単語
				wordKey.set(wordStr.substring(0, 1), wordStr);
				word.set(wordStr);
				context.write(wordKey, ONE);
			}
		}
	}
}
