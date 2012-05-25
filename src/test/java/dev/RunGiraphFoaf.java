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

package dev;

import org.apache.giraph.utils.InternalVertexRunner;
import org.apache.jena.grande.giraph.FoafShortestPathsVertex;
import org.apache.jena.grande.giraph.TurtleVertexInputFormat;
import org.apache.jena.grande.giraph.TurtleVertexOutputFormat;

public class RunGiraphFoaf {

	public static final String[] data = new String[] {
		"<http://example.org/alice> <http://xmlns.com/foaf/0.1/mbox> <mailto:alice@example.org>; <http://xmlns.com/foaf/0.1/name> \"Alice\"; <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Person>; <http://xmlns.com/foaf/0.1/knows> <http://example.org/charlie>, <http://example.org/bob>, <http://example.org/snoopy>; . <http://example.org/charlie> <http://xmlns.com/foaf/0.1/knows> <http://example.org/alice>.", 
		"<http://example.org/bob> <http://xmlns.com/foaf/0.1/name> \"Bob\"; <http://xmlns.com/foaf/0.1/knows> <http://example.org/charlie>; . <http://example.org/alice> <http://xmlns.com/foaf/0.1/knows> <http://example.org/bob>.", 
		"<http://example.org/charlie> <http://xmlns.com/foaf/0.1/name> \"Charlie\"; <http://xmlns.com/foaf/0.1/knows> <http://example.org/alice>; . <http://example.org/bob> <http://xmlns.com/foaf/0.1/knows> <http://example.org/charlie>. <http://example.org/alice> <http://xmlns.com/foaf/0.1/knows> <http://example.org/charlie>.",
	};
	
	public static void main(String[] args) throws Exception {
	    Iterable<String> results = InternalVertexRunner.run(
	    	FoafShortestPathsVertex.class,
	        TurtleVertexInputFormat.class,
	        TurtleVertexOutputFormat.class,
	        RunGiraphTest.params, 
	        data
	    );


	}
	
}
