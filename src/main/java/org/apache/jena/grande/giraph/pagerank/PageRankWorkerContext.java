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

import org.apache.giraph.graph.WorkerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PageRankWorkerContext extends WorkerContext {

	private static final Logger log = LoggerFactory.getLogger(PageRankWorkerContext.class);
	
	@Override
	public void preApplication() throws InstantiationException, IllegalAccessException {
		if ( log.isDebugEnabled() ) logAggregators("preApplication", "");
	}

	@Override
	public void postApplication() {
		if ( log.isDebugEnabled() ) logAggregators("postApplication", "");
	}
	
	@Override
	public void preSuperstep() {
		if ( log.isDebugEnabled() ) logAggregators("preSuperstep", "");
	}

	@Override
	public void postSuperstep() {
		if ( log.isDebugEnabled() ) logAggregators("postSuperstep", "");
	}

	private void logAggregators(String method, String msg) {
		log.debug("{}() {}", method, msg);
		log.debug("{}() pagerank-sum = {}", method, getAggregatedValue("pagerank-sum"));
		log.debug("{}() vertices-count = {}", method, getAggregatedValue("vertices-count"));
		log.debug("{}() error-previous = {}", method, getAggregatedValue("error-previous"));
		log.debug("{}() error-current = {}", method, getAggregatedValue("error-current"));
		log.debug("{}() dangling-current = {}", method, getAggregatedValue("dangling-current"));
		log.debug("{}() dangling-previous = {}", method, getAggregatedValue("dangling-previous"));
	}

}