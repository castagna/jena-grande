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

package dev;

import java.util.HashMap;

import org.apache.jena.grande.giraph.sssps.SingleSourceShortestPaths2;
import org.apache.jena.grande.giraph.sssps.TextTextNullTextTextVertexInputFormat;

public class RunGiraphSSSPS2 {

	public static final String[] data = new String[] {
		"1 2 5", 
		"2 1 3 5", 
		"3 2 4", 
		"4 3 5 6", 
		"5 1 2 4", 
		"6 4", 
	};
	
	public static void main(String[] args) throws Exception {
	    Iterable<String> results = MyInternalVertexRunner.run(
	    	SingleSourceShortestPaths2.class,
	        TextTextNullTextTextVertexInputFormat.class,
	        MyIdWithValueTextOutputFormat.class,
	        new HashMap<String,String>(),
	        data
	    );
	    
	    for (String result : results) {
			System.out.println(result);
		}

	    System.exit(0);
	}
	
}
