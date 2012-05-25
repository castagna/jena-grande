/*
 * Copyright © 2011 Talis Systems Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.jena.grande.giraph.pagerank.memory;

public class Node {

	private String id ;
	
	public Node(String id) {
		this.id = id ;
	}
	
	public String getId() {
		return id ;
	}
	
    public boolean equals ( Object obj ) {
		return ((Node)obj).getId().equals(id) ;
	}
    
    public int hashCode() {
    	return id.hashCode() ;
    }
	
}
