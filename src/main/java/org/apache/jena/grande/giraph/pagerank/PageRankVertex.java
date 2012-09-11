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

import org.apache.giraph.graph.EdgeListVertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PageRankVertex extends EdgeListVertex<Text, DoubleWritable, NullWritable, DoubleWritable> {

	private static final Logger log = LoggerFactory.getLogger(PageRankVertex.class); 

	public static final int DEFAULT_NUM_ITERATIONS = 30;
	public static final float DEFAULT_TOLERANCE = 10e-9f;

	private int numIterations;
	private double tolerance;
	
	@Override
	public void compute(Iterable<DoubleWritable> msgIterator) {
		log.debug("{}#{} compute() vertexValue={}", new Object[]{getId(), getSuperstep(), getValue()});

		numIterations = getConf().getInt("giraph.pagerank.iterations", DEFAULT_NUM_ITERATIONS);
		tolerance = getConf().getFloat("giraph.pagerank.tolerance", DEFAULT_TOLERANCE);
		
		if ( getSuperstep() == 0 ) {
			log.debug("{}#{} compute(): sending fake messages to count vertices, including 'implicit' dangling ones", getId(), getSuperstep());
			sendMessageToAllEdges ( new DoubleWritable() );
		} else if ( getSuperstep() == 1 ) {
			log.debug("{}#{} compute(): counting vertices including 'implicit' dangling ones", getId(), getSuperstep());
			aggregate ( "vertices-count", new LongWritable(1L) );
			aggregate ( "error-current", new DoubleWritable(Double.MAX_VALUE) );
		} else if ( getSuperstep() == 2 ) {
			long numVertices = ((LongWritable)getAggregatedValue("vertices-count")).get();
			aggregate ( "error-current", new DoubleWritable(Double.MAX_VALUE) );
			log.debug("{}#{} compute(): initializing pagerank scores to 1/N, N={}", new Object[]{getId(), getSuperstep(), numVertices});
			DoubleWritable vertexValue = new DoubleWritable ( 1.0 / numVertices );
			setValue(vertexValue);			
			log.debug("{}#{} compute() vertexValue <-- {}", new Object[]{getId(), getSuperstep(), getValue()});
			sendMessages();
		} else if ( getSuperstep() > 2 ) {
			long numVertices = ((LongWritable)getAggregatedValue("vertices-count")).get();
			double sum = 0;
			for ( DoubleWritable msgValue : msgIterator ) {
				log.debug("{}#{} compute() <-- {}", new Object[]{getId(), getSuperstep(), msgValue});				
				sum += msgValue.get();
			}
			double danglingNodesContribution = ((DoubleWritable)getAggregatedValue("dangling-previous")).get();
			DoubleWritable vertexValue = new DoubleWritable( ( 0.15f / numVertices ) + 0.85f * ( sum + danglingNodesContribution / numVertices ) );
			aggregate( "error-current", new DoubleWritable(Math.abs(vertexValue.get() - getValue().get())) );
			setValue(vertexValue);
			log.debug("{}#{} compute() vertexValue <-- {}", new Object[]{getId(), getSuperstep(), getValue()});
			sendMessages();				
		}
	}

	private void sendMessages() {
		if ( ( getSuperstep() - 3 < numIterations ) && ( ((DoubleWritable)getAggregatedValue("error-previous")).get() > tolerance ) ) {
			long edges = getNumEdges();
			if ( edges > 0 )  {
				sendMessageToAllEdges ( new DoubleWritable(getValue().get() / edges) );
			} else {
				aggregate( "dangling-current", getValue() );
			}
		} else {
			aggregate ( "pagerank-sum", getValue() );
			voteToHalt();
			log.debug("{}#{} compute() --> halt", getId(), getSuperstep());
		}
	}

}
