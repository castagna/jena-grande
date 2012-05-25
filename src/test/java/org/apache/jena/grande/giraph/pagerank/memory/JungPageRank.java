/*
 * Cloud9: A MapReduce Library for Hadoop
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

// package edu.umd.cloud9.demo;

// Taken from here: https://subversion.umiacs.umd.edu/umd-hadoop/core/trunk/src/edu/umd/cloud9/demo/SequentialPageRank.java

package org.apache.jena.grande.giraph.pagerank.memory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.jena.grande.MemoryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.uci.ics.jung.algorithms.scoring.PageRank;
import edu.uci.ics.jung.graph.DirectedSparseGraph;

/**
 * <p>
 * Program that computes PageRank for a graph using the <a
 * href="http://jung.sourceforge.net/">JUNG</a> package (2.0 alpha1). Program
 * takes two command-line arguments: the first is a file containing the graph
 * data, and the second is the random jump factor (a typical setting is 0.15).
 * </p>
 * 
 * <p>
 * The graph should be represented as an adjacency list. Each line should have
 * at least one token; tokens should be tab delimited. The first token
 * represents the unique id of the source node; subsequent tokens represent its
 * link targets (i.e., outlinks from the source node). For completeness, there
 * should be a line representing all nodes, even nodes without outlinks (those
 * lines will simply contain one token, the source node id).
 * </p>
 * 
 */
public class JungPageRank {
	
	private static final Logger LOG = LoggerFactory.getLogger(JungPageRank.class);
	
	private File input;
	private int maxIterations;
	private double tolerance;
	private double alpha;

	public JungPageRank(File input, int maxIterations, double tolerance, double alpha) {
		this.input = input;
		this.maxIterations = maxIterations;
		this.tolerance = tolerance;
		this.alpha = alpha;
	}

	private void load_data (BufferedReader in, DirectedSparseGraph<String, Integer> graph) throws IOException {
		long start = System.currentTimeMillis() ;
		int edgeCnt = 0;
		String line;
		while ((line = in.readLine()) != null) {
			StringTokenizer st = new StringTokenizer(line);
			String source = null;
			if ( st.hasMoreTokens() ) {
				source = st.nextToken();
				if (graph.addVertex(source))
					LOG.debug(String.format("Added verted %s to the graph", source));
			}

			HashSet<String> seen = new HashSet<String>() ;
			while ( st.hasMoreTokens() ) {
				String destination = st.nextToken();
				if (graph.addVertex(destination)) // implicit dangling nodes
					LOG.debug(String.format("Added verted %s to the graph", destination));				
				if (!destination.equals(source)) { // no self-references
					if (!seen.contains(destination)) { // no duplicate links
						graph.addEdge(new Integer(edgeCnt++), source, destination);
						seen.add(destination) ;
					}
				}
			}
		}
		in.close();
		
		LOG.debug(String.format("Loaded %d nodes and %d links in %d ms", graph.getVertexCount(), graph.getEdgeCount(), (System.currentTimeMillis()-start)));
	}
	
	public Map<String, Double> compute() throws IOException {
		MemoryUtil.printUsedMemory() ;
		
		DirectedSparseGraph<String, Integer> graph = new DirectedSparseGraph<String, Integer>();
		BufferedReader data = new BufferedReader(new InputStreamReader(new FileInputStream(input)));
		load_data(data, graph);

		MemoryUtil.printUsedMemory() ;
		
		long start = System.currentTimeMillis() ;
		PageRank<String, Integer> ranker = new PageRank<String, Integer>(graph, alpha);
		ranker.setTolerance(this.tolerance) ;
		ranker.setMaxIterations(this.maxIterations);

		ranker.evaluate();

		LOG.debug ("Tolerance = " + ranker.getTolerance() );
		LOG.debug ("Dump factor = " + (1.00d - ranker.getAlpha() ) ) ;
		LOG.debug ("Max iterations = " + ranker.getMaxIterations() ) ;
		LOG.debug ("PageRank computed in " + (System.currentTimeMillis()-start) + " ms"); 

		MemoryUtil.printUsedMemory() ;
				
		Map<String, Double> result = new HashMap<String, Double>();
		for (String v : graph.getVertices()) {
			result.put(v, ranker.getVertexScore(v));
		}
		return result;
	}

}
