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

package org.apache.jena.grande.examples;

import java.io.IOException;

import org.apache.pig.PigServer;

public class RunPig {
	
	private static final String output = "./target/output";

	public static void main(String[] args) throws IOException {
		PigServer pig = new PigServer("local");
		pig.deleteFile(output);
		// pig.debugOn();
		// pig.registerJar("./target/jena-grande-0.1-SNAPSHOT.jar");
		pig.registerQuery("quads = LOAD './src/test/resources/data.nq' USING org.apache.jena.grande.pig.RdfStorage() AS (g,s,p,o);");
		pig.registerQuery("a = FILTER quads BY ( p == '<http://xmlns.com/foaf/0.1/name>' ) ;");
		pig.registerQuery("b = FILTER a BY ( o == '\"Bob\"' ) ;");
		pig.store("b", output, "org.apache.jena.grande.pig.RdfStorage()");
	}

}
