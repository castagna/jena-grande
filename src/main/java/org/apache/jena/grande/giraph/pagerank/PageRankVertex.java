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
	public static final int NUM_ITERATIONS = 30;

	@Override
	public void compute(Iterator<DoubleWritable> msgIterator) {
		log.debug("{}#{} - compute(...) vertexValue={}", new Object[]{getVertexId(), getSuperstep(), getVertexValue()});

		@SuppressWarnings("unchecked")
		Aggregator<DoubleWritable> danglingAggegator = (Aggregator<DoubleWritable>)getAggregator("dangling");
		@SuppressWarnings("unchecked")
		Aggregator<DoubleWritable> pagerankAggegator = (Aggregator<DoubleWritable>)getAggregator("pagerank");
		@SuppressWarnings("unchecked")
		Aggregator<DoubleWritable> errorCurrentAggegator = (Aggregator<DoubleWritable>)getAggregator("error-current");
		@SuppressWarnings("unchecked")
		Aggregator<DoubleWritable> errorPreviousAggegator = (Aggregator<DoubleWritable>)getAggregator("error-previous");
		log.debug("{}#{} - compute(...) errorCurrentAggregator={}", new Object[]{getVertexId(), getSuperstep(), errorCurrentAggegator.getAggregatedValue() });
		log.debug("{}#{} - compute(...) errorPreviousAggregator={}", new Object[]{getVertexId(), getSuperstep(), errorPreviousAggegator.getAggregatedValue() });

		@SuppressWarnings("unchecked")
		Aggregator<LongWritable> countVerticesAggegator = (Aggregator<LongWritable>)getAggregator("count");
		long numVertices = countVerticesAggegator.getAggregatedValue().get();
		
		double danglingNodesContribution = danglingAggegator.getAggregatedValue().get();
		
		if ( getSuperstep() == 0 ) {
			log.debug("{}#{} - compute(...): {}", new Object[]{getVertexId(), getSuperstep(), "sending fake messages, just to count vertices including dangling ones"});
			sendMsgToAllEdges ( new DoubleWritable() );
		} else if ( getSuperstep() == 1 ) {
			log.debug("{}#{} - compute(...): {}", new Object[]{getVertexId(), getSuperstep(), "counting vertices including dangling ones"});
			countVerticesAggegator.aggregate(new LongWritable(1L));
		} else if ( getSuperstep() == 2 ) {
			log.debug("{}#{} - compute(...): numVertices={}", new Object[]{getVertexId(), getSuperstep(), numVertices});
			log.debug("{}#{} - compute(...): {}", new Object[]{getVertexId(), getSuperstep(), "initializing pagerank scores to 1/N"});
			DoubleWritable vertexValue = new DoubleWritable ( 1.0 / numVertices );
			setVertexValue(vertexValue);			
			log.debug("{}#{} - compute(...) vertexValue={}", new Object[]{getVertexId(), getSuperstep(), getVertexValue()});
			send( danglingAggegator, pagerankAggegator );
		} else if ( getSuperstep() > 2 ) {
			if ( getSuperstep() % 2 == 1 ) {
				log.debug("{}#{} - compute(...): numVertices={}", new Object[]{getVertexId(), getSuperstep(), numVertices});
				double sum = 0;
				while (msgIterator.hasNext()) {
					double msgValue = msgIterator.next().get(); 
					log.debug("{}#{} - compute(...) <-- {}", new Object[]{getVertexId(), getSuperstep(), msgValue});				
					sum += msgValue;
				}
				log.debug("{}#{} - compute(...) danglingNodesContribution={}", new Object[]{getVertexId(), getSuperstep(), danglingNodesContribution });
				DoubleWritable vertexValue = new DoubleWritable( ( 0.15f / numVertices ) + 0.85f * ( sum + danglingNodesContribution / numVertices ) );
				errorCurrentAggegator.aggregate( new DoubleWritable(Math.abs(vertexValue.get() - getVertexValue().get())) );
				setVertexValue(vertexValue);
				log.debug("{}#{} - compute(...) vertexValue={}", new Object[]{getVertexId(), getSuperstep(), getVertexValue()});
			}
			send( danglingAggegator, pagerankAggegator );				
		}
	}
	
	private void send( Aggregator<DoubleWritable> danglingAggegator, Aggregator<DoubleWritable> pagerankAggegator ) {
		if ( getSuperstep() < NUM_ITERATIONS ) {
			long edges = getNumOutEdges();
			if ( edges > 0 )  {
				log.debug("{}#{} - send(...) numOutEdges={} propagating pagerank values...", new Object[]{getVertexId(), getSuperstep(), edges});
				sendMsgToAllEdges ( new DoubleWritable(getVertexValue().get() / edges) );
			}
			if ( ( edges == 0 ) && ( getSuperstep() % 2 == 0) ) {
				log.debug("{}#{} - send(...) numOutEdges={} updating dangling node contribution...", new Object[]{getVertexId(), getSuperstep(), edges});
				log.debug("{}#{} - send(...) danglingAggregator={}", new Object[]{getVertexId(), getSuperstep(), danglingAggegator.getAggregatedValue().get()});
				danglingAggegator.aggregate( getVertexValue() );
				log.debug("{}#{} - send(...) danglingAggregator={}", new Object[]{getVertexId(), getSuperstep(), danglingAggegator.getAggregatedValue().get()});
			}
		} else {
			pagerankAggegator.aggregate ( getVertexValue() );
			voteToHalt();
			log.debug("{}#{} - compute(...) --> halt", new Object[]{getVertexId(), getSuperstep()});
		}
	}

}
