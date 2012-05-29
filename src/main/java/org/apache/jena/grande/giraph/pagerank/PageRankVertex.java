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

import java.util.Iterator;

import org.apache.giraph.graph.Aggregator;
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
	private Aggregator<?> danglingCurrentAggegator;
	private Aggregator<?> pagerankSumAggegator;

	@Override
	public void compute(Iterator<DoubleWritable> msgIterator) {
		log.debug("{}#{} compute() vertexValue={}", new Object[]{getVertexId(), getSuperstep(), getVertexValue()});

		danglingCurrentAggegator = getAggregator("dangling-current");
		@SuppressWarnings("unchecked") Aggregator<DoubleWritable> errorCurrentAggegator = (Aggregator<DoubleWritable>)getAggregator("error-current");
		pagerankSumAggegator = getAggregator("pagerank-sum");
		@SuppressWarnings("unchecked") Aggregator<LongWritable> verticesCountAggregator = (Aggregator<LongWritable>)getAggregator("vertices-count");

		long numVertices = verticesCountAggregator.getAggregatedValue().get();
		double danglingNodesContribution = PageRankVertexWorkerContext.danglingPrevious;
		numIterations = getConf().getInt("giraph.pagerank.iterations", DEFAULT_NUM_ITERATIONS);
		tolerance = getConf().getFloat("giraph.pagerank.tolerance", DEFAULT_TOLERANCE);
		
		if ( getSuperstep() == 0 ) {
			log.debug("{}#{} compute(): sending fake messages to count vertices, including 'implicit' dangling ones", getVertexId(), getSuperstep());
			sendMsgToAllEdges ( new DoubleWritable() );
		} else if ( getSuperstep() == 1 ) {
			log.debug("{}#{} compute(): counting vertices including 'implicit' dangling ones", getVertexId(), getSuperstep());
			verticesCountAggregator.aggregate ( new LongWritable(1L) );
		} else if ( getSuperstep() == 2 ) {
			log.debug("{}#{} compute(): initializing pagerank scores to 1/N, N={}", new Object[]{getVertexId(), getSuperstep(), numVertices});
			DoubleWritable vertexValue = new DoubleWritable ( 1.0 / numVertices );
			setVertexValue(vertexValue);			
			log.debug("{}#{} compute() vertexValue <-- {}", new Object[]{getVertexId(), getSuperstep(), getVertexValue()});
			sendMessages();
		} else if ( getSuperstep() > 2 ) {
			double sum = 0;
			while (msgIterator.hasNext()) {
				double msgValue = msgIterator.next().get(); 
				log.debug("{}#{} compute() <-- {}", new Object[]{getVertexId(), getSuperstep(), msgValue});				
				sum += msgValue;
			}
			DoubleWritable vertexValue = new DoubleWritable( ( 0.15f / numVertices ) + 0.85f * ( sum + danglingNodesContribution / numVertices ) );
			errorCurrentAggegator.aggregate( new DoubleWritable(Math.abs(vertexValue.get() - getVertexValue().get())) );
			setVertexValue(vertexValue);
			log.debug("{}#{} compute() vertexValue <-- {}", new Object[]{getVertexId(), getSuperstep(), getVertexValue()});
			sendMessages();				
		}
	}

	@SuppressWarnings("unchecked")
	private void sendMessages() {
		if ( ( getSuperstep() - 3 < numIterations ) && ( PageRankVertexWorkerContext.errorPrevious > tolerance ) ) {
			long edges = getNumOutEdges();
			if ( edges > 0 )  {
				sendMsgToAllEdges ( new DoubleWritable(getVertexValue().get() / edges) );
			} else {
				((Aggregator<DoubleWritable>)danglingCurrentAggegator).aggregate( getVertexValue() );
			}
		} else {
			((Aggregator<DoubleWritable>)pagerankSumAggegator).aggregate ( getVertexValue() );
			voteToHalt();
			log.debug("{}#{} compute() --> halt", getVertexId(), getSuperstep());
		}
	}

}
