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
import java.util.Map;

import org.apache.giraph.graph.BasicVertex;
import org.apache.giraph.graph.BspUtils;
import org.apache.giraph.lib.TextVertexInputFormat.TextVertexReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.jena.grande.NodeEncoder;
import org.apache.jena.grande.mapreduce.io.NodeWritable;
import org.openjena.riot.Lang;
import org.openjena.riot.RiotLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.vocabulary.FOAF;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;

public class TurtleVertexReader extends TextVertexReader<NodeWritable, IntWritable, NodeWritable, IntWritable> {

	private static final Logger log = LoggerFactory.getLogger(TurtleVertexWriter.class);
	
	public TurtleVertexReader(RecordReader<LongWritable, Text> lineRecordReader) {
		super(lineRecordReader);
		log.debug("TurtleVertexReader({})", lineRecordReader.toString());
	}

	@Override
	public boolean nextVertex() throws IOException, InterruptedException {
		boolean result = getRecordReader().nextKeyValue();
		log.debug("nextVertex() --> {}", result);
		return result;
	}

	@Override
	public BasicVertex<NodeWritable, IntWritable, NodeWritable, IntWritable> getCurrentVertex() throws IOException, InterruptedException {
		Configuration conf = getContext().getConfiguration();
		BasicVertex<NodeWritable, IntWritable, NodeWritable, IntWritable> vertex = BspUtils.createVertex(conf);
		Text line = getRecordReader().getCurrentValue();
		NodeWritable vertexId = getVertexId(line);
		Graph graph = RiotLoader.graphFromString(line.toString(), Lang.TURTLE, "");
		Map<NodeWritable, NodeWritable> edgeMap = getEdgeMap(vertexId, graph);
		vertex.initialize(vertexId, null, edgeMap, null);
		log.debug("getCurrentVertex() --> {}", vertex);
		return vertex;
	}
	
	private NodeWritable getVertexId( Text line ) {
		String str = line.toString();
		NodeWritable vertexId = new NodeWritable(NodeEncoder.asNode(str.substring(0, str.indexOf(' '))));
		log.debug("getVertexId({}) --> {}", line, vertexId);
		return vertexId;
	}

	private Map<NodeWritable, NodeWritable> getEdgeMap( NodeWritable vertexId, Graph graph ) {
		log.debug("getEdgeMap({}, {})", vertexId, graph);
		Node s = vertexId.getNode();
		Map<NodeWritable, NodeWritable> edgeMap = Maps.newHashMap();
		ExtendedIterator<Triple> iter = graph.find(s, FOAF.knows.asNode(), Node.ANY);
		while ( iter.hasNext() ) {
			Triple triple = iter.next();
			NodeWritable o = new NodeWritable(triple.getObject());
			NodeWritable p = new NodeWritable(triple.getPredicate());
			log.debug("getEdgeMap: adding {} {}", o, p);
			edgeMap.put(o, p);
		}
		return edgeMap;
	}

}
