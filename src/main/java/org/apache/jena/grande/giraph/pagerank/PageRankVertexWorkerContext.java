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

import org.apache.giraph.aggregators.DoubleSumAggregator;
import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.graph.Aggregator;
import org.apache.giraph.graph.DefaultMasterCompute;
import org.apache.giraph.graph.WorkerContext;
import org.apache.hadoop.io.DoubleWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PageRankVertexWorkerContext extends WorkerContext {

	private static final Logger log = LoggerFactory.getLogger(PageRankVertexWorkerContext.class);

	public static double errorPrevious = Double.MAX_VALUE;
	public static double danglingPrevious = 0d;
	
	// TODO: double check this... how is calling initialize()?
	public static class SimplePageRankVertexMasterCompute extends DefaultMasterCompute {
		@Override
		public void initialize() throws InstantiationException, IllegalAccessException {
			registerAggregator("dangling-current", DoubleSumAggregator.class);
			registerAggregator("error-current", DoubleSumAggregator.class);
			registerAggregator("pagerank-sum", DoubleSumAggregator.class);
			registerAggregator("vertices-count", LongSumAggregator.class);
		}
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public void preApplication() throws InstantiationException, IllegalAccessException {
		log.debug("preApplication()");
		((Aggregator<DoubleWritable>)getAggregatedValue("error-current")).setAggregatedValue( new DoubleWritable( Double.MAX_VALUE ) );
	}

	@Override
	public void postApplication() {
		log.debug("postApplication()");
		log.debug("postApplication() pagerank-sum={}", getAggregatedValue("pagerank-sum"));
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public void preSuperstep() {
		log.debug("preSuperstep()");
		if ( getSuperstep() > 2 ) {
			errorPrevious = ((Aggregator<DoubleWritable>)getAggregatedValue("error-current")).getAggregatedValue().get();
			((Aggregator<DoubleWritable>)getAggregatedValue("error-current")).setAggregatedValue( new DoubleWritable(0L) );
		}
		danglingPrevious = ((Aggregator<DoubleWritable>)getAggregatedValue("dangling-current")).getAggregatedValue().get();
		((Aggregator<DoubleWritable>)getAggregatedValue("dangling-current")).setAggregatedValue( new DoubleWritable(0L) );
	}

	@Override
	public void postSuperstep() {
		log.debug("postSuperstep()");
	}

}