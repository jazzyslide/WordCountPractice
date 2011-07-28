package com.lifexweb.app.hadoop.WordCount.V1;

import java.util.Arrays;
import java.util.List;

import junit.framework.TestCase;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.lifexweb.app.hadoop.WordCount.V1.WordCountReducer;

public class WordCountReducerTest extends TestCase {

	private WordCountReducer reducer;
	private ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
	private List<IntWritable> counts;
	private static final IntWritable ONE = new IntWritable(1);
	
	@Before
	public void setUp() throws Exception {
		reducer = new WordCountReducer();
		reduceDriver = new ReduceDriver<Text, IntWritable, Text, IntWritable>(reducer);
	}

	@Test
	public void testReduce() {
		counts = Arrays.asList(ONE,ONE,ONE,ONE);
		reduceDriver.withInput(new Text("word"), counts)
		.withOutput(new Text("word"), new IntWritable(4))
		.runTest();
		reduceDriver.resetOutput();
		
		counts = Arrays.asList(ONE,ONE,ONE,ONE,ONE);
		reduceDriver.withInput(new Text("pen"), counts)
		.withOutput(new Text("pen"), new IntWritable(5))
		.runTest();
		reduceDriver.resetOutput();		
	}

}
