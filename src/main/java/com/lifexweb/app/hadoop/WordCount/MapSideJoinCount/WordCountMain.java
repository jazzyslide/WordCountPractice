/**
 * 
 */
package com.lifexweb.app.hadoop.WordCount.MapSideJoinCount;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * @author kato_hideya
 * WordCount(MapSideJoin)の実行用Mainクラス
 */
public class WordCountMain {

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
		//設定情報の読み込み
		Configuration conf = new Configuration();
		
		//DistributedCache
		DistributedCache.addCacheFile(URI.create(args[2]), conf);
		
		//引数のパース
		GenericOptionsParser parser = new GenericOptionsParser(conf, args);
		args = parser.getRemainingArgs();
		
		Job job = new Job(conf, "WordCount MapSideJoin Ver.");
		
		job.setJarByClass(WordCountMain.class);
		
		job.setMapperClass(WordCountMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setNumReduceTasks(1);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		boolean success = job.waitForCompletion(true);
		if (success) System.out.println("Job SUCCESS!!!");
	}

}
