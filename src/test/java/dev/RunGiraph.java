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

package dev;

import java.util.Map;

import junit.framework.Assert;

import org.apache.giraph.examples.SimpleShortestPathsVertex;
import org.apache.giraph.utils.InternalVertexRunner;


public class RunGiraph {

	public static void main(String[] args) throws Exception {
	    Iterable<String> results = InternalVertexRunner.run(
	    	SimpleShortestPathsVertex.class,
	        SimpleShortestPathsVertex.SimpleShortestPathsVertexInputFormat.class,
	        SimpleShortestPathsVertex.SimpleShortestPathsVertexOutputFormat.class,
	        RunGiraphTest.params, 
	        RunGiraphTest.graph
	    );

	    Map<Long, Double> distances = RunGiraphTest.parseDistances(results);
	    Assert.assertNotNull(distances);
	    Assert.assertEquals(4, distances.size());
	    Assert.assertEquals(0.0, distances.get(1L));
	    Assert.assertEquals(1.0, distances.get(2L));
	    Assert.assertEquals(2.0, distances.get(3L));
	    Assert.assertEquals(4.0, distances.get(4L));
	}
	
}
