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

package org.apache.jena.grande;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemoryUtil {

	private static final Logger LOG = LoggerFactory.getLogger(MemoryUtil.class);
	
	public static long getUsedMemory() {
		System.gc() ;
		System.gc() ;
		System.gc() ;
		
		return ( Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory() ) / ( 1024 * 1024 ) ;
	}
	
	public static void printUsedMemory() {
		LOG.debug (String.format("memory used is %d MB", getUsedMemory())) ;
	}
	
}
