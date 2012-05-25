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

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.jena.grande.Constants;
import org.apache.jena.grande.JenaGrandeException;
import org.openjena.atlas.event.Event;
import org.openjena.atlas.event.EventListener;
import org.openjena.atlas.event.EventManager;
import org.openjena.atlas.event.EventType;

public class Counters implements EventListener {

	private final TaskInputOutputContext<?,?,?,?> context;
	
    private Counter counterTriples = null;
    private Counter counterQuads = null;
    private Counter counterDuplicates = null;
    private Counter counterMalformed = null;
    private Counter counterRdfNodes = null;
    private Counter counterRecords = null;
    
    private long numTriples = 0L;
    private long numQuads = 0L;
    private long numDuplicates = 0L;
    private long numMalformed = 0L;
    private long numRdfNodes = 0L;
    private long numRecords = 0L;
    private long n = 0L;
    
    private int tick;

	public Counters (TaskInputOutputContext<?,?,?,?> context) {
		this(context, 1000);
	}

	public Counters (TaskInputOutputContext<?,?,?,?> context, int tick) {
		this.context = context;
		this.tick = tick;

		EventManager.register(this, Constants.eventQuad, this);
		EventManager.register(this, Constants.eventTriple, this);
		EventManager.register(this, Constants.eventDuplicate, this);
		EventManager.register(this, Constants.eventMalformed, this);
		EventManager.register(this, Constants.eventRdfNode, this);
		EventManager.register(this, Constants.eventRecord, this);
	}

	public void setTick(int tick) {
		this.tick = tick;
	}
	
	public int getTick() {
		return tick;
	}

	@Override
	public void event(Object dest, Event event) {
		if ( this.equals(dest) ) {
			EventType type = event.getType();
			if ( type.equals(Constants.eventQuad) ) {
				if ( counterQuads == null ) counterQuads = context.getCounter(Constants.JENA_GRANDE_COUNTER_GROUPNAME, Constants.JENA_GRANDE_COUNTER_QUADS);
				numQuads++;
			} else if ( type.equals(Constants.eventTriple) ) {
				if ( counterTriples == null ) counterTriples = context.getCounter(Constants.JENA_GRANDE_COUNTER_GROUPNAME, Constants.JENA_GRANDE_COUNTER_TRIPLES);
				numTriples++;
			} else if ( type.equals(Constants.eventDuplicate) ) {
				if ( counterDuplicates == null ) counterDuplicates = context.getCounter(Constants.JENA_GRANDE_COUNTER_GROUPNAME, Constants.JENA_GRANDE_COUNTER_DUPLICATES);
				numDuplicates++;
			} else if ( type.equals(Constants.eventMalformed) ) {
				if ( counterMalformed == null ) counterMalformed = context.getCounter(Constants.JENA_GRANDE_COUNTER_GROUPNAME, Constants.JENA_GRANDE_COUNTER_MALFORMED);
				numMalformed++;
			} else if ( type.equals(Constants.eventRdfNode) ) {
				if ( counterRdfNodes == null ) counterRdfNodes = context.getCounter(Constants.JENA_GRANDE_COUNTER_GROUPNAME, Constants.JENA_GRANDE_COUNTER_RDFNODES);
				numRdfNodes++;
			} else if ( type.equals(Constants.eventRecord) ) {
				if ( counterRecords == null ) counterRecords = context.getCounter(Constants.JENA_GRANDE_COUNTER_GROUPNAME, Constants.JENA_GRANDE_COUNTER_RECORDS);
				numRecords++;
			} else {
				throw new JenaGrandeException("Unsupported event type: " + type);
			}
			n++;
			report();
		}		
	}
	
	public void close() {
		increment();
	}
	
	private void report() {
		if ( n > tick ) {
			increment();
		}
	}
	
	private void increment() {
		if ( ( numTriples != 0L ) && ( counterTriples != null ) ) counterTriples.increment(numTriples);
		if ( ( numQuads != 0L ) && ( counterQuads != null ) ) counterQuads.increment(numQuads);
		if ( ( numMalformed != 0L ) && ( counterMalformed != null ) ) counterMalformed.increment(numMalformed);
		if ( ( numDuplicates != 0L ) && ( counterDuplicates != null ) ) counterDuplicates.increment(numDuplicates);
		if ( ( numRdfNodes != 0L ) && ( counterRdfNodes != null ) ) counterRdfNodes.increment(numRdfNodes);
		if ( ( numRecords != 0L ) && ( counterRecords != null ) ) counterRecords.increment(numRecords);

	    numTriples = 0L;
	    numQuads = 0L;
	    numDuplicates = 0L;
	    numMalformed = 0L;
	    numRdfNodes = 0L;
	    numRecords = 0L;

		EventManager.send(null, new Event(Constants.eventTick, n));

		n = 0L;
	}
	
}
