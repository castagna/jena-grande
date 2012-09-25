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
import org.apache.giraph.io.TextVertexOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.jena.grande.mapreduce.io.NodeWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TurtleVertexOutputFormat extends TextVertexOutputFormat<NodeWritable, IntWritable, NodeWritable> {

	private static final Logger log = LoggerFactory.getLogger(TurtleVertexOutputFormat.class);
	
	@Override
	public TextVertexWriter createVertexWriter(TaskAttemptContext context) throws IOException, InterruptedException {
		TextVertexWriter result = new TurtleVertexWriter();
		log.debug("createVertexWriter({}) --> {}", context, result);
		return result;
	}
	
	public class TurtleVertexWriter extends TextVertexWriter {

		private final Logger log = LoggerFactory.getLogger(TurtleVertexWriter.class);

		@Override
		public void writeVertex(Vertex<NodeWritable, IntWritable, NodeWritable, ?> vertex) throws IOException, InterruptedException {
			log.info("write({},{})", new Object[]{vertex.getId(), vertex.getValue()});
			// TODO: this must be done differently, or not?
			getRecordWriter().write (null, null);
		}

	}
	
}
