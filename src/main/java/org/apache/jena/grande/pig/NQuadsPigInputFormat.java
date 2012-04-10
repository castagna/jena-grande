package org.apache.jena.grande.pig;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.jena.grande.mapreduce.io.QuadRecordReader;
import org.apache.jena.grande.mapreduce.io.QuadWritable;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigFileInputFormat;

public class NQuadsPigInputFormat extends PigFileInputFormat<LongWritable, QuadWritable> {

	@Override
	public RecordReader<LongWritable, QuadWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		return new QuadRecordReader();
	}

}
