package org.apache.jena.grande.pig;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.jena.grande.mapreduce.io.TripleWritable;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigFileInputFormat;

public class NTriplesPigInputFormat extends PigFileInputFormat<LongWritable, TripleWritable> {

	@Override
	public RecordReader<LongWritable, TripleWritable> createRecordReader(InputSplit arg0, TaskAttemptContext arg1) throws IOException, InterruptedException {
		return null;
	}

}
