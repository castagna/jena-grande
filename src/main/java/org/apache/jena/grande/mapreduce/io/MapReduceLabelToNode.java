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

package org.apache.jena.grande.mapreduce.io;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.jena.grande.Constants;
import org.openjena.riot.lang.LabelToNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.rdf.model.AnonId;

public class MapReduceLabelToNode extends LabelToNode { 

	private static final Logger log = LoggerFactory.getLogger(MapReduceLabelToNode.class);
	
    public MapReduceLabelToNode(JobContext context, Path path) {
        super(new SingleScopePolicy(), new MapReduceAllocator(context, path));
    }
    
    private static class SingleScopePolicy implements ScopePolicy<String, Node, Node> { 
        private Map<String, Node> map = new HashMap<String, Node>() ;
        @Override public Map<String, Node> getScope(Node scope) { return map ; }
        @Override public void clear() { map.clear(); }
    }
    
    private static class MapReduceAllocator implements Allocator<String, Node> {
        
        private String runId ;
        private Path path ;

        public MapReduceAllocator (JobContext context, Path path) {
        	// This is to ensure that blank node allocation policy is constant when subsequent MapReduce jobs need that
            this.runId = context.getConfiguration().get(Constants.RUN_ID);
            if ( this.runId == null ) {
            	this.runId = String.valueOf(System.currentTimeMillis());
            	log.warn("runId was not set, it has now been set to {}. Sequence of MapReduce jobs must handle carefully blank nodes.", runId);
            }
            this.path = path;
        	log.debug("MapReduceAllocator({}, {})", runId, path) ;
        }

        @Override 
        public Node create(String label) {
        	String strLabel = "mrbnode_" + runId.hashCode() + "_" + path.hashCode() + "_" + label;
        	log.debug ("create({}) = {}", label, strLabel);
            return Node.createAnon(new AnonId(strLabel)) ;
        }

        @Override public void reset() {}
    };
    
}
