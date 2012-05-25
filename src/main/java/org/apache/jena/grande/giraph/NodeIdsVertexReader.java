/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
