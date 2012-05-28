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

import org.apache.giraph.examples.LongSumAggregator;
import org.apache.giraph.examples.SumAggregator;
import org.apache.giraph.graph.Aggregator;
import org.apache.giraph.graph.WorkerContext;
import org.apache.hadoop.io.DoubleWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PageRankVertexWorkerContext extends WorkerContext {

	private static final Logger log = LoggerFactory.getLogger(PageRankVertexWorkerContext.class);

	@Override
	public void preApplication() throws InstantiationException, IllegalAccessException {
		log.debug("preApplication()");
		registerAggregator("dangling", SumAggregator.class);
		registerAggregator("pagerank", SumAggregator.class);
		registerAggregator("error", SumAggregator.class);
		registerAggregator("count", LongSumAggregator.class);
	}

	@Override
	public void postApplication() {
		log.debug("postApplication()");
		log.debug("postApplication() pagerank={}", getAggregator("pagerank").getAggregatedValue());
	}

	@Override
	public void preSuperstep() {
		log.debug("preSuperstep()");
		if ( getSuperstep() % 2 == 0 ) {
			((Aggregator<DoubleWritable>)getAggregator("dangling")).setAggregatedValue(new DoubleWritable(0L));
			log.debug("preSuperstep() danglingAggregators={}", getAggregator("dangling").getAggregatedValue());
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void postSuperstep() {
		log.debug("postSuperstep()");
	}

}