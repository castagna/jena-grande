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

package org.apache.jena.grande.pig;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.util.List;

import org.apache.jena.grande.NodeEncoder;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.algebra.OpVisitorBase;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;
import com.hp.hpl.jena.util.FileUtils;

public class Sparql2PigLatinWriter extends OpVisitorBase {

	private Writer out = null;
	
	public Sparql2PigLatinWriter(Writer out) {
		super();
		this.out = out;
	}
	
	public Sparql2PigLatinWriter(OutputStream out) {
		this(FileUtils.asPrintWriterUTF8(out));
	}
	
    public void visit(OpBGP opBGP) {
    	try {
        	List<Triple> patterns = opBGP.getPattern().getList();
        	boolean first = true;
        	
        	for (Triple triple : patterns) {
        		String filter = null;

        		if (!triple.getSubject().isVariable()) {
        			if (first) filter = "FILTER dataset BY ";
        			filter.concat("( s == '" + out(triple.getSubject()) + "' )");
        			first = false;
        		} 
        		
        		if (!triple.getPredicate().isVariable()) {
        			if ( first ) {
            			filter = "FILTER dataset BY ";
        			} else {
        				filter = filter.concat(" AND ") ;
        			}
        			filter = filter.concat("( p == '" + out(triple.getPredicate()) + "' )");
        			first = false;        			
        		}

        		if (!triple.getObject().isVariable()) {
        			if ( first ) {
            			filter = "FILTER dataset BY ";
        			} else {
        				filter = filter.concat(" AND ") ;
        			}
        			filter = filter.concat("( o == '" + out(triple.getObject()) + "' )");
        			first = false;
        		}
        		
        		if (filter != null) {
        			out.write(filter);
        		}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
    }

    private String out(Node node) {
    	return NodeEncoder.asString(node);
    }

}
