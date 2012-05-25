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

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.jena.grande.hbase.HBaseQuadSink;
import org.apache.jena.grande.hbase.HBaseRdfConnection;
import org.apache.jena.grande.hbase.HBaseRdfConnectionFactory;
import org.openjena.atlas.lib.Sink;
import org.openjena.riot.Lang;
import org.openjena.riot.RiotLoader;

import com.hp.hpl.jena.sparql.core.Quad;

public class RunHBaseRdf {

	private final static HBaseTestingUtility testing = new HBaseTestingUtility();
//	private final static String filename = "src/test/resources/data.nq";
	private final static String filename = "/opt/datasets/raw/foodista/foodista.nt";
	
	public static void main(String[] args) throws Exception {
		MiniHBaseCluster cluster = testing.startMiniCluster();
		Configuration configuration = cluster.getConfiguration();
		HBaseRdfConnection connection = HBaseRdfConnectionFactory.create(configuration);
		HTable table = connection.createTable("SPOG", Arrays.asList("P", "O", "G"));

		InputStream input = new FileInputStream(filename);
		Sink<Quad> sink = new HBaseQuadSink(table);
		RiotLoader.readQuads(input, Lang.NQUADS, "", sink);
		sink.close();
		
		cluster.shutdown();
	}
	
}
