package org.apache.jena.grande.giraph;

import java.io.IOException;

import org.apache.giraph.graph.BasicVertex;
import org.apache.giraph.graph.BspUtils;
import org.apache.giraph.lib.TextVertexInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.RecordReader;

public class NodeIdsVertexReader<V extends Writable, M extends Writable> extends TextVertexInputFormat.TextVertexReader<LongWritable, V, LongWritable, M> {

	public NodeIdsVertexReader(RecordReader<LongWritable, Text> lineRecordReader) {
		super(lineRecordReader);
	}

	@Override
	public boolean nextVertex() throws IOException, InterruptedException {
		return getRecordReader().nextKeyValue();
	}

	@Override
	public BasicVertex<LongWritable, V, LongWritable, M> getCurrentVertex() throws IOException, InterruptedException {
		BasicVertex<LongWritable, V, LongWritable, M> vertex = BspUtils.createVertex(getContext().getConfiguration());
		LongWritable key = getRecordReader().getCurrentKey();
		Text value = getRecordReader().getCurrentValue();
		

		
		return null;
	}

}
