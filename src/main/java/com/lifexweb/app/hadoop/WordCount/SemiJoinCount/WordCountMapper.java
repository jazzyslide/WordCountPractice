package com.lifexweb.app.hadoop.WordCount.SemiJoinCount;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, WordKeyWritable, WordValueWritable> {
	
	private static final String PATTERN = "[-a-zA-Z0-9]+";
    private static final Pattern pattern = Pattern.compile(PATTERN);
    private static final String SYMLINK = "links";
 
    private WordKeyWritable resultKey = new WordKeyWritable();
    private WordValueWritable resultValue = new WordValueWritable();
    private HashMap<String,String> wordUrlPair = new HashMap<String, String>();
    private HashSet<String> countedWords = new HashSet<String>();

	private static final String URL_RECORD_FLAG_OFF = "0";
	private static final String URL_RECORD_FLAG_ON = "1";
    
	private static Log log = LogFactory.getLog(WordCountMapper.class);

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
		log.info("[MAP:WordCountMapper] map task started... JobId: " + context.getJobID().getId());
		
		//DistributedCacheでキーワードとURLのマッピングをパース
		wordUrlPair = parseCacheFile(context);
	}
    
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		
		//正規表現でワードを抜き出すかたちに
		Matcher matcher = pattern.matcher(line);
		while (matcher.find()) {
			for (String wordStr : matcher.group().split("\\s")){				
				if (wordUrlPair.containsKey(wordStr)) {
					//word&Urlのペアにwordがあった場合：URL行もword行も出力
					
					//URL行
					resultKey.set(wordStr, URL_RECORD_FLAG_ON);
					resultValue.set(wordStr, wordUrlPair.get(wordStr), 0);
					context.write(resultKey, resultValue);
					
					//キーワード行
					resultKey.set(wordStr, URL_RECORD_FLAG_OFF);
					resultValue.set(wordStr, "", 1);
					context.write(resultKey, resultValue);
					
					//出力済みのURL行はHashMapから削除。
					//これ以降のこのwordに関するword行は出力対象のためwordを
					//URL出力済みwordのHashSetに格納
					countedWords.add(wordStr);
					wordUrlPair.remove(wordStr);
				} else if (countedWords.contains(wordStr)) {
					//URLを出力済みのwordの場合：word行だけ出力
					resultKey.set(wordStr, URL_RECORD_FLAG_OFF);
					resultValue.set(wordStr, "", 1);
					context.write(resultKey, resultValue);
				}
			}
		}
	}
	
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		log.info("[MAP:WordCountMapper] map task finished... JobId: " + context.getJobID().getId());
		super.cleanup(context);
	}

	//DistributedCacheをパースしてHashMapに格納
	private HashMap<String,String> parseCacheFile(Context context) throws IOException {
		HashMap<String,String> retPair = new HashMap<String, String>();
		log.info("[MAP:WordCountMapper]: cacheFile symlink: " + SYMLINK);
		
		BufferedReader br = null;
		try {
			br = new BufferedReader(new InputStreamReader(new FileInputStream(new File(SYMLINK))));			
			String line = br.readLine();
			while (line != null) {
				String[] pair = line.split("\t");
				if (pair.length == 2) {
					retPair.put(pair[0], pair[1]);
				}
				line = br.readLine();
			}
		} finally {
			if (br != null) {
				br.close();	
			}
		}
		return retPair;
	}
}
