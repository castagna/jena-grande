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

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.TextVertexInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class NodeIdsVertexInputFormat<V extends Writable, M extends Writable> extends TextVertexInputFormat<LongWritable, V, LongWritable, M> {

	@Override
	public TextVertexReader createVertexReader(InputSplit split, TaskAttemptContext context) throws IOException {
	    return new NodeIdsVertexReader();
	}

	public class NodeIdsVertexReader extends TextVertexInputFormat<LongWritable, V, LongWritable, M> .TextVertexReader {

		@Override
		public boolean nextVertex() throws IOException, InterruptedException {
			return getRecordReader().nextKeyValue();
		}

		@Override
		public Vertex<LongWritable, V, LongWritable, M> getCurrentVertex() throws IOException, InterruptedException {
//			Vertex<LongWritable, V, LongWritable, M> vertex = BspUtils.createVertex(getContext().getConfiguration());
//			LongWritable key = getRecordReader().getCurrentKey();
//			Text value = getRecordReader().getCurrentValue();

			return null;
		}

	}
	
}
