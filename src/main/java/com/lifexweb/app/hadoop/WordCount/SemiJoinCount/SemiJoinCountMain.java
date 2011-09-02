package com.lifexweb.app.hadoop.WordCount.SemiJoinCount;

import java.net.URI;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SemiJoinCountMain extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		if (args.length != 3) {
			System.err.println("Usage: [generic options] <INPUT> <OUTPUT> <DIST CACHE FILE>");
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}

		JobConf conf = new JobConf(getConf(), getClass());
		Job job = new Job(conf, "WordCount Semi-Join Ver.");
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//Distributed Cache
		DistributedCache.addCacheFile(URI.create(args[2]), job.getConfiguration());
		DistributedCache.createSymlink(job.getConfiguration());		
		
		job.setMapOutputKeyClass(WordKeyWritable.class);
		job.setMapOutputValueClass(WordValueWritable.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(WordValueWritable.class);
		
		job.setMapperClass(WordCountMapper.class);
		job.setPartitionerClass(WordPartitioner.class);
		job.setSortComparatorClass(UrlFlagSortComparator.class);
		job.setCombinerClass(WordCountCombiner.class);
		job.setGroupingComparatorClass(WordComparator.class);
		job.setReducerClass(WordCountReducer.class);
		
		job.setNumReduceTasks(1);
		
		boolean success = job.waitForCompletion(true);
		if (success) System.out.println("Job SUCCESS!!!");
		return 0;
	}

	/**
	 * @param args: <INPUT> <OUTPUT> <DIST CACHE FILE>
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new SemiJoinCountMain(), args);
		System.exit(exitCode);
	}

}
