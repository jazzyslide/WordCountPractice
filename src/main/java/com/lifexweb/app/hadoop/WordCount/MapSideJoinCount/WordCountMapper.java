package com.lifexweb.app.hadoop.WordCount.MapSideJoinCount;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	private static final String PATTERN = "[-a-zA-Z0-9]+";
    private static final Pattern pattern = Pattern.compile(PATTERN);
 
    private Text resultKey = new Text();
    private static final IntWritable ONE = new IntWritable(1);
    private HashMap<String,String> wordUrlPair = new HashMap<String, String>();
    
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
				if (wordUrlPair.get(wordStr) != null) {
					resultKey.set(wordStr + "\t" + wordUrlPair.get(wordStr));
					context.write(resultKey, ONE);
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
		
		FileSystem fs = FileSystem.get(context.getConfiguration());
		URI[] cacheFilePaths = DistributedCache.getCacheFiles(context.getConfiguration());
		HashMap<String,String> retPair = new HashMap<String, String>();
		
		if (cacheFilePaths != null) {
			for (URI uri : cacheFilePaths) {
				
				log.info("[MAP:WordCountMapper]: cacheFile URI: " + uri.toString());
				
				BufferedReader br = null;
				try {
					br = new BufferedReader(new InputStreamReader(fs.open(new Path(uri))));			
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
				
			}
		}
		return retPair;
	}
}
