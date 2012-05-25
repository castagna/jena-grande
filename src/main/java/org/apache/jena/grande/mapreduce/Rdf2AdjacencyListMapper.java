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

package org.apache.jena.grande.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.jena.grande.Constants;
import org.apache.jena.grande.mapreduce.io.NodeWritable;
import org.apache.jena.grande.mapreduce.io.QuadWritable;
import org.openjena.atlas.event.Event;
import org.openjena.atlas.event.EventManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.Quad;

public class Rdf2AdjacencyListMapper extends Mapper<LongWritable, QuadWritable, NodeWritable, QuadWritable> {

    private static final Logger log = LoggerFactory.getLogger(Rdf2AdjacencyListMapper.class);
    private Counters counters;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
    	counters = new Counters(context);
    }

    @Override
	public void map (LongWritable key, QuadWritable value, Context context) throws IOException, InterruptedException {
        log.debug("< ({}, {})", key, value);

        Quad quad = value.getQuad();
        if ( quad.isTriple() ) {
    		EventManager.send(counters, new Event(Constants.eventTriple, null));
        } else {
    		EventManager.send(counters, new Event(Constants.eventQuad, null));
        }
		Node s = quad.getSubject();
        emit (s, value, context);

        Node o = quad.getObject();
        if ( o.isURI() ) {
        	emit (o, value, context);
        }
	}
    
    private void emit (Node node, QuadWritable quadWritable, Context context) throws IOException, InterruptedException {
		NodeWritable nodeWritable = new NodeWritable(node);
		context.write(nodeWritable, quadWritable);
		log.debug("> ({}, {})", nodeWritable, quadWritable);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    	counters.close();
    }

}
