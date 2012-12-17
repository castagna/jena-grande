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

package org.apache.jena.grande.giraph.pagerank;

import java.io.IOException;
import java.util.Map;

import org.apache.giraph.graph.BspUtils;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.TextVertexInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

public class PageRankVertexInputFormat extends TextVertexInputFormat<Text, DoubleWritable, NullWritable, DoubleWritable> {

	private static final Logger log = LoggerFactory.getLogger(PageRankVertexReader.class);
	
	@Override
	public TextVertexReader createVertexReader(InputSplit split, TaskAttemptContext context) throws IOException {
		return new PageRankVertexReader();
	}
	
	public class PageRankVertexReader extends TextVertexInputFormat<Text, DoubleWritable, NullWritable, DoubleWritable>.TextVertexReader {

		@Override
		public boolean nextVertex() throws IOException, InterruptedException {
			boolean result = getRecordReader().nextKeyValue(); 
			log.debug("nextVertex() --> {}", result);
			return result;
		}

		@Override
		public Vertex<Text, DoubleWritable, NullWritable, DoubleWritable> getCurrentVertex() throws IOException, InterruptedException {
		    Configuration conf = getContext().getConfiguration();
		    String line = getRecordReader().getCurrentValue().toString();
		    Vertex<Text, DoubleWritable, NullWritable, DoubleWritable> vertex = BspUtils.createVertex(conf);

		    String tokens[] = line.split("[\t ]"); // TODO: make this configurable
		    Text vertexId = new Text(tokens[0]);
		    DoubleWritable vertexValue = new DoubleWritable(0); // TODO: at this point we do not know the number of nodes in the graph :-/

		    Map<Text, NullWritable> edges = Maps.newHashMap();
		    for ( int i = 1; i < tokens.length; i++ ) {
		    	if ( !tokens[0].equals(tokens[i]) ) { // no self-links
		    		edges.put ( new Text(tokens[i]), NullWritable.get() );
		    	}
		    }

		    vertex.initialize ( vertexId, vertexValue, edges );
			log.debug("getCurrentVertex() --> {}", vertex);
		    return vertex;
		}
	}

}
