/**
 * 
 */
package com.lifexweb.app.hadoop.WordCount.V2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.lifexweb.app.hadoop.WordCount.V2.Writable.WordKeyWritable;

/**
 * @author kato_hideya
 * WordCount ver.2の実行用Mainクラス
 */
public class WordCountMain {

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		//設定情報の読み込み
		Configuration conf = new Configuration();
		
		//引数のパース
		GenericOptionsParser parser = new GenericOptionsParser(conf, args);
		args = parser.getRemainingArgs();
	
		Job job = new Job(conf, "WordCount v2");
		job.setJarByClass(WordCountMain.class);
		
		job.setMapperClass(WordCountMapper.class);
		job.setPartitionerClass(CapitalLetterPartitioner.class);
		job.setCombinerClass(WordCountCombiner.class);
		job.setGroupingComparatorClass(CapitalLetterComparator.class);
		job.setReducerClass(WordCountReducer.class);
		
		job.setMapOutputKeyClass(WordKeyWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setNumReduceTasks(3);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		boolean success = job.waitForCompletion(true);
		if (success) System.out.println("Job SUCCESS!!!");
	}

}
