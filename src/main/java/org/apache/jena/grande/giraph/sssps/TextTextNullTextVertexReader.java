/**
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

package org.apache.jena.grande.giraph.sssps;

import java.io.IOException;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.giraph.graph.BasicVertex;
import org.apache.giraph.graph.BspUtils;
import org.apache.giraph.lib.TextVertexInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordReader;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class TextTextNullTextVertexReader  extends TextVertexInputFormat.TextVertexReader<Text, Text, NullWritable, Text> {

	private static final Pattern SEPARATOR = Pattern.compile("[\t ]");

	public TextTextNullTextVertexReader(RecordReader<LongWritable, Text> lineReader) {
		super(lineReader);
	}

	@Override
	public boolean nextVertex() throws IOException, InterruptedException {
		return getRecordReader().nextKeyValue();
	}

	@Override
	public BasicVertex<Text, Text, NullWritable, Text> getCurrentVertex() throws IOException, InterruptedException {
		BasicVertex<Text, Text, NullWritable, Text> vertex = BspUtils.<Text, Text, NullWritable, Text>createVertex(getContext().getConfiguration());
		String[] tokens = SEPARATOR.split(getRecordReader().getCurrentValue().toString());
		Map<Text, NullWritable> edges = Maps.newHashMapWithExpectedSize(tokens.length - 1);
		for (int n = 1; n < tokens.length; n++) {
	        edges.put(new Text(tokens[n]), NullWritable.get());
		}
		Text vertexId = new Text(tokens[0]);
		vertex.initialize(vertexId, vertexId, edges, Lists.<Text>newArrayList());
		return vertex;
	}

}
