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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.giraph.GiraphConfiguration;

import dev.MyInternalVertexRunner;

public class RunPageRankVertexLocally {

	public static Map<String, Double> run ( String filename ) throws Exception {		
		Map<String,String> params = new HashMap<String,String>();
		params.put(GiraphConfiguration.WORKER_CONTEXT_CLASS, "org.apache.jena.grande.giraph.pagerank.PageRankWorkerContext");
		params.put(GiraphConfiguration.MASTER_COMPUTE_CLASS, "org.apache.jena.grande.giraph.pagerank.PageRankMasterCompute");

		String[] data = getData ( filename );
	    Iterable<String> results = MyInternalVertexRunner.run(
	    	PageRankVertex.class,
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
	
	static String[] getData( String filename ) throws IOException {
		BufferedReader in = null;
		try {
			in = new BufferedReader(new FileReader(filename));
			String line = null;
			ArrayList<String> data = new ArrayList<String>();
			while ( ( line = in.readLine() ) != null ) {
				data.add(line);
			}
			return data.toArray(new String[]{});
		} finally {
			if ( in != null ) in.close();
		}
	}
	
}
