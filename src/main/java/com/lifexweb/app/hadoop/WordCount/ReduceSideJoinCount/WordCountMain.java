/**
 * 
 */
package com.lifexweb.app.hadoop.WordCount.ReduceSideJoinCount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * @author kato_hideya
 * WordCount(ReduceSideJoin)の実行用Mainクラス
 */
public class WordCountMain {

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		//設定情報の読み込み
		Configuration conf = new Configuration();
		
		//引数のパース
		GenericOptionsParser parser = new GenericOptionsParser(conf, args);
		args = parser.getRemainingArgs();
	
		Job job = new Job(conf, "WordCount ReduceSideJoin Ver.");
		job.setJarByClass(WordCountMain.class);
		
		job.setMapperClass(WordCountMapper.class);
		job.setPartitionerClass(WordPartitioner.class);
		job.setCombinerClass(WordCountCombiner.class);
		job.setSortComparatorClass(WordSortComparator.class);
		job.setGroupingComparatorClass(WordComparator.class);
		job.setReducerClass(WordCountReducer.class);
		
		job.setMapOutputKeyClass(WordKeyWritable.class);
		job.setMapOutputValueClass(WordValueWritable.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(WordValueWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setNumReduceTasks(3);
		
		MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class, WordCountMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, SiteUrlMapper.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		boolean success = job.waitForCompletion(true);
		if (success) System.out.println("Job SUCCESS!!!");
	}

}
