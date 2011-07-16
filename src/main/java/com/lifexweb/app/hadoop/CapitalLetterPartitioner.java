package com.lifexweb.app.hadoop;

import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class CapitalLetterPartitioner extends Partitioner<Text, IntWritable> {

	private static final String A_TO_M_PATTERN = "^\'*[a-mA-M].*";
	private static final String N_TO_Z_PATTERN = "^\'*[n-zN-Z].*";
//	private static final String NUMERIC_PATTERN = "^[0-9].*";
	private Pattern a_to_m_pattern = Pattern.compile(A_TO_M_PATTERN);
	private Pattern n_to_z_pattern = Pattern.compile(N_TO_Z_PATTERN);
//	private Pattern numeric_pattern = Pattern.compile(NUMERIC_PATTERN);
	
	
	@Override
	public int getPartition(Text word, IntWritable count, int numReducer) {
		if (a_to_m_pattern.matcher(word.toString()).matches()){
			return 0;
		} else if (n_to_z_pattern.matcher(word.toString()).matches()) {
			return 1;
		} else {
			return 2;
		}
	}

}
