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

package org.apache.jena.grande;

import org.openjena.atlas.event.EventType;
import org.openjena.riot.system.PrefixMap;

import com.hp.hpl.jena.sparql.vocabulary.FOAF;
import com.hp.hpl.jena.vocabulary.DC;
import com.hp.hpl.jena.vocabulary.DCTerms;
import com.hp.hpl.jena.vocabulary.DCTypes;
import com.hp.hpl.jena.vocabulary.OWL;
import com.hp.hpl.jena.vocabulary.RDF;
import com.hp.hpl.jena.vocabulary.RDFS;
import com.hp.hpl.jena.vocabulary.XSD;

public class Constants {

	public static final String RUN_ID = "runId";

	public static final String OPTION_USE_COMPRESSION = "useCompression";
	public static final boolean OPTION_USE_COMPRESSION_DEFAULT = false;

	public static final String OPTION_OVERRIDE_OUTPUT = "overrideOutput";
	public static final boolean OPTION_OVERRIDE_OUTPUT_DEFAULT = false;

	public static final String OPTION_RUN_LOCAL = "runLocal";
	public static final boolean OPTION_RUN_LOCAL_DEFAULT = false;

	public static final String OPTION_NUM_REDUCERS = "numReducers";
	public static final int OPTION_NUM_REDUCERS_DEFAULT = 20;
	
	public static final String RDF_2_ADJACENCY_LIST = "rdf2adjacencylist";


	public static final PrefixMap defaultPrefixMap = new PrefixMap();
	static {
		defaultPrefixMap.add("rdf", RDF.getURI());
		defaultPrefixMap.add("rdfs", RDFS.getURI());
		defaultPrefixMap.add("owl", OWL.getURI());
		defaultPrefixMap.add("xsd", XSD.getURI());
		defaultPrefixMap.add("foaf", FOAF.getURI());
		defaultPrefixMap.add("dc", DC.getURI());
		defaultPrefixMap.add("dcterms", DCTerms.getURI());
		defaultPrefixMap.add("dctypes", DCTypes.getURI());
	}

	// MapReduce counters
	
	public static final String JENA_GRANDE_COUNTER_GROUPNAME = "TDBLoader4 Counters";
	public static final String JENA_GRANDE_COUNTER_MALFORMED = "Malformed";
	public static final String JENA_GRANDE_COUNTER_QUADS = "Quads (including duplicates)";
	public static final String JENA_GRANDE_COUNTER_TRIPLES = "Triples (including duplicates)";
	public static final String JENA_GRANDE_COUNTER_DUPLICATES = "Duplicates (quads or triples)";
	public static final String JENA_GRANDE_COUNTER_RDFNODES = "RDF nodes";
	public static final String JENA_GRANDE_COUNTER_RECORDS = "Index records";
	
	// Event types
	
	public static EventType eventQuad = new EventType("quad");
	public static EventType eventTriple = new EventType("triple");
	public static EventType eventDuplicate = new EventType("duplicate");
	public static EventType eventMalformed = new EventType("malformed");
	public static EventType eventRdfNode = new EventType("RDF node");
	public static EventType eventRecord = new EventType("record");
	public static EventType eventTick = new EventType("tick");
	
}
