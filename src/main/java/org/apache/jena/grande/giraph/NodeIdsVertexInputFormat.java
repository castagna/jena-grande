package org.apache.jena.grande.giraph;

import java.io.IOException;

import org.apache.giraph.graph.VertexReader;
import org.apache.giraph.lib.TextVertexInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class NodeIdsVertexInputFormat<V extends Writable, M extends Writable> extends TextVertexInputFormat<LongWritable, V, LongWritable, M> {

	@Override
	public VertexReader<LongWritable, V, LongWritable, M> createVertexReader(InputSplit split, TaskAttemptContext context) throws IOException {
	    return new NodeIdsVertexReader<V, M>(textInputFormat.createRecordReader(split, context));
	}

}
