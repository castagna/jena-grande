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

import java.util.HashMap;
import java.util.Map;

import org.apache.giraph.utils.InternalVertexRunner;

public class RunSimplePageRankVertexLocally {

	public static Map<String, Double> run ( String filename ) throws Exception {		
		Map<String,String> params = new HashMap<String,String>();

		String[] data = RunPageRankVertexLocally.getData ( filename );
	    Iterable<String> results = InternalVertexRunner.run(
	    	SimplePageRankVertex.class,
	        PageRankVertexInputFormat.class,
	        PageRankVertexOutputFormat.class,
	        params,
	        data
	    );

		Map<String,Double> pageranks = new HashMap<String,Double>();
	    for ( String result : results ) {
	    	String[] tokens = result.split("[\t ]");
	    	if ( tokens.length == 2 ) {
	    		pageranks.put(tokens[0], Double.valueOf(tokens[1]));
	    	}
		}		
		return pageranks;
	}
	
	public static void main(String[] args) throws Exception {
		Map<String, Double> pageranks = run ( "src/test/resources/pagerank.txt" );

		for ( String vertex : pageranks.keySet() ) {
			System.out.println ( vertex + "\t" + pageranks.get(vertex) );
		}

	    System.exit(0);
	}
	
}
