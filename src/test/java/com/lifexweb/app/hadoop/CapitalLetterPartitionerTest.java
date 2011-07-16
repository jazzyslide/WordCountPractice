package com.lifexweb.app.hadoop;

import static org.junit.Assert.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

public class CapitalLetterPartitionerTest {

	private CapitalLetterPartitioner partitioner;
	
	@Before
	public void setUp() throws Exception {
		partitioner = new CapitalLetterPartitioner();
	}

	@Test
	public final void testGetPartitionTextIntWritableInt() {
		assertEquals(partitioner.getPartition(new Text("a"), new IntWritable(1), 3), 0);
		assertEquals(partitioner.getPartition(new Text("apple"), new IntWritable(1), 3), 0);
		assertEquals(partitioner.getPartition(new Text("night"), new IntWritable(1), 3), 1);
		assertEquals(partitioner.getPartition(new Text("123"), new IntWritable(1), 3), 2);
		assertEquals(partitioner.getPartition(new Text("'test"), new IntWritable(1), 3), 1);
	}


}
