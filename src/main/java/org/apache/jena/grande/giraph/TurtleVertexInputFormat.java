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

import org.apache.giraph.graph.VertexReader;
import org.apache.giraph.lib.TextVertexInputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.jena.grande.mapreduce.io.NodeWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TurtleVertexInputFormat extends TextVertexInputFormat<NodeWritable, IntWritable, NodeWritable, IntWritable> {

	private static final Logger log = LoggerFactory.getLogger(TurtleVertexInputFormat.class);
	
	@Override
	public VertexReader<NodeWritable, IntWritable, NodeWritable, IntWritable> createVertexReader(InputSplit split, TaskAttemptContext context) throws IOException {
		VertexReader<NodeWritable, IntWritable, NodeWritable, IntWritable> result = new TurtleVertexReader(textInputFormat.createRecordReader(split, context));
		log.debug("createVertexReader({}, {}) --> {}", new Object[]{split, context, result});
	    return result;
	}

}
