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

package org.apache.jena.grande.examples;

import java.util.Map;

import junit.framework.TestCase;

import org.apache.giraph.examples.SimpleShortestPathsVertex;
import org.apache.giraph.utils.InternalVertexRunner;
import org.json.JSONArray;
import org.json.JSONException;

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

public class RunGiraphTest extends TestCase {

    public static final String[] graph = new String[] {
	        "[1,0,[[2,1],[3,3]]]",
	        "[2,0,[[3,1],[4,10]]]",
	        "[3,0,[[4,2]]]",
	    	"[4,0,[]]" 
	};

	public static final Map<String, String> params = Maps.newHashMap();
	static {
		params.put(SimpleShortestPathsVertex.SOURCE_ID, "1");
	}
	
	public void testRun() throws Exception {
	    Iterable<String> results = InternalVertexRunner.run(
	    	SimpleShortestPathsVertex.class,
	        SimpleShortestPathsVertex.SimpleShortestPathsVertexInputFormat.class,
	        SimpleShortestPathsVertex.SimpleShortestPathsVertexOutputFormat.class,
	        params, 
	        graph
	    );
	    
	    Map<Long, Double> distances = parseDistances(results);
	    assertNotNull(distances);
	    assertEquals(4, distances.size());
	    assertEquals(0.0, distances.get(1L));
	    assertEquals(1.0, distances.get(2L));
	    assertEquals(2.0, distances.get(3L));
	    assertEquals(4.0, distances.get(4L));
	}

	public static Map<Long, Double> parseDistances(Iterable<String> results) {
		Map<Long, Double> distances = Maps.newHashMapWithExpectedSize(Iterables.size(results));
		for (String line : results) {
			try {
				JSONArray jsonVertex = new JSONArray(line);
				distances.put(jsonVertex.getLong(0), jsonVertex.getDouble(1));
			} catch (JSONException e) {
				throw new IllegalArgumentException("Couldn't get vertex from line " + line, e);
			}
		}
		return distances;
	}

}
