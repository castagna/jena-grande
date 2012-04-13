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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Iterator;

import org.openjena.atlas.lib.Sink;
import org.openjena.atlas.lib.Tuple;
import org.openjena.atlas.logging.ProgressLogger;
import org.openjena.riot.Lang;
import org.openjena.riot.RiotLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.Quad;
import com.hp.hpl.jena.sparql.util.Timer;
import com.hp.hpl.jena.tdb.base.file.Location;
import com.hp.hpl.jena.tdb.index.TupleIndex;
import com.hp.hpl.jena.tdb.nodetable.NodeTable;
import com.hp.hpl.jena.tdb.store.NodeId;
import com.hp.hpl.jena.tdb.store.QuadTable;
import com.hp.hpl.jena.tdb.store.TripleTable;
import com.hp.hpl.jena.tdb.sys.DatasetControlNone;
import com.hp.hpl.jena.tdb.sys.Names;
import com.hp.hpl.jena.tdb.sys.SetupTDB;
import com.hp.hpl.jena.tdb.sys.SystemTDB;

public class RunTdbLowLevel {

	// private final static String filename = "/opt/datasets/raw/foodista/foodista.nt";
	private final static String filename = "src/test/resources/data2.nt";
	
	public static void main(String[] args) throws FileNotFoundException {
		Location location = new Location("target/tdb");
		NodeTable nodeTable = SetupTDB.makeNodeTable(location,
                Names.indexNode2Id, SystemTDB.Node2NodeIdCacheSize ,
                Names.indexId2Node, SystemTDB.NodeId2NodeCacheSize ,
                SystemTDB.NodeMissCacheSize ) ;
		// QuadTable quadTable = SetupTDB.makeQuadTable(location, nodeTable, "SPOG", new String[]{ "SPOG" }, new DatasetControlNone()); // this as a sort of bug, it ignores a couple of parameters
        QuadTable quadTable = makeQuadTable(location, nodeTable);
        TripleTable tripleTable = makeTripleTable(location, nodeTable);
       
		InputStream input = new FileInputStream(filename);
		Sink<Quad> sink = new QuadTableSink(tripleTable, quadTable);
		RiotLoader.readQuads(input, Lang.NQUADS, "", sink);
		sink.flush();

		Timer timer = new Timer();
		timer.startTimer();
		Iterator<Quad> iter = quadTable.find(Node.ANY, Node.ANY, Node.ANY, Node.ANY);
		int count = 0;
		while ( iter.hasNext() ) {
			iter.next();
			count = count + 1;
		}
		System.out.println("Counted " + count + " quads in " + timer.endTimer() + " ms");
		
		timer.startTimer();
		Iterator<Tuple<NodeId>> iter2 = quadTable.getNodeTupleTable().findAsNodeIds(Node.ANY, Node.ANY, Node.ANY, Node.ANY);
		count = 0;
		final boolean print = false;
		while ( iter2.hasNext() ) {
			Tuple<NodeId> tuple = iter2.next();
			if ( print ) {
				for ( NodeId nodeId : tuple ) {
					System.out.print (nodeId.getId());
					System.out.print (" ");
				}
				System.out.println();				
			}
			count = count + 1;
		}
		System.out.println("Counted " + count + " ids in " + timer.endTimer() + " ms");
		
		asAdjacencyList(quadTable);
		
		sink.close();
	}
	
	private static void asAdjacencyList ( QuadTable quadTable ) {
		Timer timer = new Timer();
		timer.startTimer();
		Iterator<Tuple<NodeId>> iter = quadTable.getNodeTupleTable().findAsNodeIds(Node.ANY, Node.ANY, Node.ANY, Node.ANY);
		NodeId s_prev = null;
		NodeId p_prev = null;
		boolean s_first = true;
		while ( iter.hasNext() ) {
			Tuple<NodeId> tuple = iter.next();
			NodeId s = tuple.get(1);
			NodeId p = tuple.get(2);
			NodeId o = tuple.get(3);
			
			if ( !s.equals(s_prev) ) {
				if ( !s_first ) {
					System.out.println(" )");
				} else {
					s_first = false;
				}
				w(quadTable, s);
				System.out.print(" ( ");
				w(quadTable, p);
				System.out.print(" ");
				w(quadTable, o);
				s_prev = s;
				p_prev = p;
			} else {
				if ( !p.equals(p_prev) ) {
					System.out.print(" ) ( ");
					w(quadTable, p);
					System.out.print(" ");
					w(quadTable, o);
				} else {
					System.out.print(" ");
					w(quadTable, o);				
				}
				p_prev = p;
			}
		}
		System.out.println(" )");
		System.out.println("Elapsed time: " + timer.endTimer() + " ms");
	}

	private static void w ( QuadTable quadTable, NodeId nodeId ) {
//		Node node = quadTable.getNodeTupleTable().getNodeTable().getNodeForNodeId(nodeId);
//		System.out.print(NodeEncoder.asString(node));
		System.out.print(nodeId.getId());
	}
	
	private static QuadTable makeQuadTable(Location location, NodeTable nodeTable) {
        String primary = "SPOG" ;
        String indexes[] = new String[]{ "SPOG" } ;
        TupleIndex quadIndexes[] = SetupTDB.makeTupleIndexes(location, primary, indexes, indexes) ;
        QuadTable quadTable = new QuadTable(quadIndexes, nodeTable, new DatasetControlNone()) ;
        return quadTable;
	}

	private static TripleTable makeTripleTable(Location location, NodeTable nodeTable) {
        String primary = "SPO" ;
        String indexes[] = new String[]{ "SPO" } ;
        TupleIndex tripleIndexes[] = SetupTDB.makeTupleIndexes(location, primary, indexes, indexes) ;
        TripleTable tripleTable = new TripleTable(tripleIndexes, nodeTable, new DatasetControlNone()) ;
        return tripleTable;
	}
	
	static class QuadTableSink implements Sink<Quad> {

		private static final Logger log = LoggerFactory.getLogger( QuadTableSink.class );
		private ProgressLogger logger = new ProgressLogger(log, "quads", 1000, 10000);

		// private final NodeTupleTable nodeTupleTable;
		private final QuadTable quadTable;
		private final TripleTable tripleTable;
		
		public QuadTableSink ( TripleTable tripleTable, QuadTable quadTable ) {
			this.tripleTable = tripleTable;
			this.quadTable = quadTable;
			// this.nodeTupleTable = quadTable.getNodeTupleTable();
		}

		private boolean started = false;

		@Override
		public void send(Quad quad) {
			// nodeTupleTable.addRow(quad.getSubject(), quad.getPredicate(), quad.getObject(), quad.getGraph());
			// tripleTable.add ( quad.asTriple() );
			quadTable.add ( quad );
			if ( !started ) {
				logger.start();
				started = true;
			}
			logger.tick();
		}

		@Override
		public void flush() {
			tripleTable.sync();
			quadTable.sync();
		}

		@Override
		public void close() {
			flush();
			tripleTable.close();
			quadTable.close();
			logger.finish();
		}

	}

}
