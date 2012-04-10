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

import java.io.StringWriter;

import org.openjena.riot.out.NodeFmtLib;
import org.openjena.riot.system.PrefixMap;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.util.NodeFactory;

public class NodeEncoder {

	private static final PrefixMap emptyPrefixMap = new PrefixMap();
	
	public static String asString(Node node) {
        StringWriter sw = new StringWriter() ;
        NodeFmtLib.serialize(sw, node, null, emptyPrefixMap);
        return sw.toString() ; 
	}
	
	public static String asString(Node node, PrefixMap prefixMap) {
        StringWriter sw = new StringWriter() ;
        NodeFmtLib.serialize(sw, node, null, prefixMap);
        return sw.toString() ; 
	}

	public static Node asNode(String str) {
		return NodeFactory.parseNode(str);
	}

}
