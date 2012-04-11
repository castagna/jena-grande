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

package org.apache.jena.grande.examples;

import java.io.IOException;
import java.io.StringWriter;

import org.apache.jena.grande.pig.Sparql2PigLatinWriter;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.sparql.algebra.Algebra;
import com.hp.hpl.jena.sparql.algebra.Op;

public class RunPigSparql2PigLatin {

	public static void main(String[] args) throws IOException {
		Query query = QueryFactory.read("src/test/resources/query.rq");
//		PrintUtils.printOp(IndentedWriter.stdout, query, true);

		Op op = Algebra.compile(query);
		
		StringWriter out = new StringWriter();
		op.visit(new Sparql2PigLatinWriter(out));
		
		System.out.println(out.toString());
	}

}
