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

package org.apache.jena.grande.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.jena.grande.NodeEncoder;
import org.openjena.atlas.lib.Sink;
import org.openjena.atlas.logging.ProgressLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.sparql.core.Quad;

public class HBaseQuadSink implements Sink<Quad> {

	private static final Logger log = LoggerFactory.getLogger( HBaseQuadSink.class );
	
	// private static final byte[] family = Bytes.toBytes("SPOG");
	private static final byte[] p = Bytes.toBytes("P");
	private static final byte[] o = Bytes.toBytes("O");
	private static final byte[] g = Bytes.toBytes("G");

	private HTable table;
	private ProgressLogger logger = new ProgressLogger(log, "quads", 1000, 100000);
	private boolean started = false;

	public HBaseQuadSink ( HTable table ) {
		this.table = table;
		this.table.setAutoFlush(false);
	}
	
	@Override
	public void send ( Quad quad ) {
		Put put = new Put(NodeEncoder.asString(quad.getSubject()).getBytes());
		put.add(p, p, NodeEncoder.asString(quad.getPredicate()).getBytes());
		put.add(o, o, NodeEncoder.asString(quad.getObject()).getBytes());
		put.add(g, g, NodeEncoder.asString(quad.getGraph()).getBytes());
		try {
			table.put(put);
			if ( !started ) {
				logger.start();
				started = true;
			}
			logger.tick();
		} catch (IOException e) {
			throw new HBaseRdfException(e);
		}
	}

	@Override
	public void flush() {
		try {
			table.flushCommits();
		} catch (IOException e) {
			throw new HBaseRdfException(e);
		}
	}

	@Override
	public void close() {
		try {
			table.close();
			logger.finish();
		} catch (IOException e) {
			throw new HBaseRdfException(e);
		}
	}

}
