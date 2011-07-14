package com.lifexweb.app.hadoop;

import junit.framework.TestCase;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

public class WordCountMapperTest extends TestCase {

	private WordCountMapper mapper;
	private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
	
	@Before
	public void setUp() {
		//テスト用Mapper作成
		mapper = new WordCountMapper();
		
		//テスト用のMapDriver作成
		mapDriver = new MapDriver<LongWritable, Text, Text, IntWritable>(mapper);
	}

	@Test
	public void testMap() {
		mapDriver.withInput(new LongWritable(0), new Text("this is a pen"))
		.withOutput(new Text("this"), new IntWritable(1))
		.withOutput(new Text("is"), new IntWritable(1))
		.withOutput(new Text("a"), new IntWritable(1))
		.withOutput(new Text("pen"), new IntWritable(1))
		.runTest();
		mapDriver.resetOutput();
		
		//スペースが複数連続である場合のテスト
		mapDriver.withInput(new LongWritable(0), new Text("this is a  pen"))
		.withOutput(new Text("this"), new IntWritable(1))
		.withOutput(new Text("is"), new IntWritable(1))
		.withOutput(new Text("a"), new IntWritable(1))
		.withOutput(new Text("pen"), new IntWritable(1))
		.runTest();
		mapDriver.resetOutput();

		//ドット、カンマ、アポストロフィが入った場合のテスト
		mapDriver.withInput(new LongWritable(0), new Text("this is a pen, and a pen's holder."))
		.withOutput(new Text("this"), new IntWritable(1))
		.withOutput(new Text("is"), new IntWritable(1))
		.withOutput(new Text("a"), new IntWritable(1))
		.withOutput(new Text("pen"), new IntWritable(1))
		.withOutput(new Text("and"), new IntWritable(1))
		.withOutput(new Text("a"), new IntWritable(1))
		.withOutput(new Text("pen\'s"), new IntWritable(1))
		.withOutput(new Text("holder"), new IntWritable(1))
		.runTest();
		mapDriver.resetOutput();
		
		//全部小文字にする
		mapDriver.withInput(new LongWritable(0),  new Text("This is a pen.And a pen."))
		.withOutput(new Text("this"), new IntWritable(1))
		.withOutput(new Text("is"), new IntWritable(1))
		.withOutput(new Text("a"), new IntWritable(1))
		.withOutput(new Text("pen"), new IntWritable(1))
		.withOutput(new Text("and"), new IntWritable(1))
		.withOutput(new Text("a"), new IntWritable(1))
		.withOutput(new Text("pen"), new IntWritable(1))
		.runTest();
		mapDriver.resetOutput();
	}

}
