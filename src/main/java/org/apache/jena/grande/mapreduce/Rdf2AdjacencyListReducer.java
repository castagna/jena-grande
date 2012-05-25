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

package org.apache.jena.grande.mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.jena.grande.Constants;
import org.apache.jena.grande.NodeEncoder;
import org.apache.jena.grande.mapreduce.io.NodeWritable;
import org.apache.jena.grande.mapreduce.io.QuadWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.Quad;

public class Rdf2AdjacencyListReducer extends Reducer<NodeWritable, QuadWritable, NullWritable, Text> {
	
    private static final Logger log = LoggerFactory.getLogger(Rdf2AdjacencyListReducer.class);
	private static final NullWritable key = NullWritable.get();
	private static final Text value = new Text();
	private static final String SPACE = " ";
	private static final String COMMA= ", ";
	private static final String SEMICOLON = "; ";
	private static final String DOT = ". ";
	
	static final boolean useDefaultPrefixes = false;
	static final boolean outputBacklinks = true;
	
	@Override
	public void reduce(NodeWritable node, Iterable<QuadWritable> values, Context context) throws IOException, InterruptedException {
		Iterator<QuadWritable> iter = values.iterator();
		Map<Node,Set<Node>> incoming = new HashMap<Node,Set<Node>>();
		Map<Node,Set<Node>> outgoing = new HashMap<Node,Set<Node>>();
		Node n = node.getNode();
		while ( iter.hasNext() ) {
			QuadWritable quad = iter.next();
			log.debug("< ({}, {}", node, quad);
			Quad q = quad.getQuad();
			Node s = q.getSubject();
			Node p = q.getPredicate();
			Node o = q.getObject();
			if ( n.equals(s) ) {
				if ( !outgoing.containsKey(p) ) outgoing.put(p, new HashSet<Node>());
				outgoing.get(p).add(o);
			} else if ( n.equals(o) ) {
				if ( !incoming.containsKey(p) ) incoming.put(p, new HashSet<Node>());
				incoming.get(p).add(s);
			}
		}

		StringBuilder sb = new StringBuilder();
		if ( outgoing.keySet().size() > 0 ) {
			sb.append(encode(n));
			sb.append(SPACE);
			for (Node p : outgoing.keySet()) {
				sb.append(encode(p));
				sb.append(SPACE);
				boolean first = true;
				for (Node o : outgoing.get(p)) {
					if (!first) sb.append(COMMA); else first = false;
					sb.append(encode(o));
				}
				sb.append(SEMICOLON);
			}
			sb.append(DOT);
		}
		
		// TODO: This should be optional, it can cause problems if there are a lot of incoming links...
		if ( ( outputBacklinks ) && ( outgoing.keySet().size() > 0 ) ) {
			for (Node p : incoming.keySet()) {
				for (Node o : incoming.get(p)) {
					sb.append(encode(o));
					sb.append(SPACE);
					sb.append(encode(p));
					sb.append(SPACE);
					sb.append(encode(n));
					sb.append(DOT);				
				}
			}			
		}
		
		value.set(sb.toString());
		context.write(key, value);
		log.debug("> ({}, {})", key, value);
	}

	private String encode ( Node node ) {
		if ( useDefaultPrefixes ) {
			return NodeEncoder.asString(node, Constants.defaultPrefixMap);			
		} else {
			return NodeEncoder.asString(node);
		}
	}
	
}
