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

package org.apache.jena.grande.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class HBaseRdfConnectionFactory {

    public static HBaseRdfConnection create( String configFile, boolean isAssemblerFile ) { 
       	return new HBaseRdfConnection( configFile );
    }

    public static HBaseRdfConnection create( Configuration config ) {
    	return new HBaseRdfConnection( config );
    }
    
    public static HBaseAdmin createHBaseAdmin( String configFile ) {
    	Configuration config = HBaseConfiguration.create();
    	config.addResource( new Path( configFile ) );
    	try { 
    		return new HBaseAdmin( config );
    	} catch ( Exception e ) { 
    		throw new HBaseRdfException( "HBase exception while creating admin", e );
    	}     	    	
    }
    
    public static HBaseAdmin createHBaseAdmin( Configuration config ) {
    	try { 
    		return new HBaseAdmin( config );
    	} catch ( Exception e ) { 
    		throw new HBaseRdfException( "HBase exception while creating admin", e );
    	}
    }
    
    public static Configuration createHBaseConfiguration( String configFile ) {
    	Configuration config = HBaseConfiguration.create();
    	config.addResource( new Path( configFile ) );
    	return config;
    }

}