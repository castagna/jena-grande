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

package cmd;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.zip.GZIPOutputStream;

import org.openjena.atlas.lib.Tuple;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.sparql.util.Timer;
import com.hp.hpl.jena.sparql.vocabulary.FOAF;
import com.hp.hpl.jena.tdb.base.file.Location;
import com.hp.hpl.jena.tdb.index.TupleIndex;
import com.hp.hpl.jena.tdb.nodetable.NodeTable;
import com.hp.hpl.jena.tdb.store.NodeId;
import com.hp.hpl.jena.tdb.store.QuadTable;
import com.hp.hpl.jena.tdb.sys.DatasetControlNone;
import com.hp.hpl.jena.tdb.sys.Names;
import com.hp.hpl.jena.tdb.sys.SetupTDB;
import com.hp.hpl.jena.tdb.sys.SystemTDB;

public class tdb2al {

	public static void main(String[] args) throws FileNotFoundException, IOException {
		if ( ( !( (args.length == 2) || (args.length == 3) ) ) || ( !isExistingReadableDirectory(args[0]) ) || ( !isNonExistingFileInWritableDirectory(args[1]) )) {
			usage();
		}

		Location location = new Location(args[0]);
		NodeTable nodeTable = makeNodeTable(location); // nodes.dat and node2id.{dat|idn}
		QuadTable quadTable = makeQuadTable(location, nodeTable); // SPOG.{dat|idn}

		Property property = FOAF.knows; // default property
		if ( args.length == 3 ) {
			property = ResourceFactory.createProperty(args[2]);
		}
		
		String filename = args[1];
		PrintWriter out = null;
		if ( filename.endsWith(".gz") ) { // if filename ends with .gz we compress the output
			out = new PrintWriter(new OutputStreamWriter(new BufferedOutputStream(new GZIPOutputStream(new FileOutputStream(filename)))));
		} else {
			out = new PrintWriter(new OutputStreamWriter(new BufferedOutputStream(new FileOutputStream(filename))));
		}
		try {
			asAdjacencyList(quadTable, property, out);
		} finally {
			if ( out != null ) out.close();
		}
	}

	private static void asAdjacencyList(QuadTable quadTable, Property property, PrintWriter out) {
		Timer timer = new Timer();
		timer.startTimer();
		NodeId s_prev = null;
		boolean first = true;
		long count = 0L;
		Set<NodeId> nodes = new HashSet<NodeId>();
		Iterator<Tuple<NodeId>> iter = quadTable.getNodeTupleTable().findAsNodeIds(Node.ANY, property.asNode(), Node.ANY, Node.ANY);
		while ( iter.hasNext() ) {
			Tuple<NodeId> tuple = iter.next();
			NodeId s = tuple.get(0);
			NodeId o = tuple.get(2);
			if ( !s.equals(s_prev) ) {
				if ( first ) {
					first = false;
				} else {
					output(s_prev, nodes, out);
					nodes = new HashSet<NodeId>();
				}
			}
			s_prev = s;
			nodes.add(o);
			count++;
		}
		output (s_prev, nodes, out);
		System.out.println("Found " + count + " links in " + timer.endTimer() + " ms");
	}

	// this writes just node ids (i.e. in practice an anonymised output)
	private static void output ( NodeId subject, Set<NodeId> nodes, PrintWriter out ) {
		out.print(subject.getId());
		for (NodeId node : nodes) {
			out.print(" ");
			out.print(node.getId());
		}
		out.print("\n");
	}
	
	private static NodeTable makeNodeTable(Location location) {
		NodeTable nodeTable = SetupTDB.makeNodeTable(location, 
				Names.indexNode2Id, SystemTDB.Node2NodeIdCacheSize ,
                Names.indexId2Node, SystemTDB.NodeId2NodeCacheSize ,
                SystemTDB.NodeMissCacheSize ) ;
		return nodeTable;
	}
	
	private static QuadTable makeQuadTable(Location location, NodeTable nodeTable) {
        String primary = "SPOG" ;
        String indexes[] = new String[]{ "SPOG" } ;
        TupleIndex quadIndexes[] = SetupTDB.makeTupleIndexes(location, primary, indexes, indexes) ;
        QuadTable quadTable = new QuadTable(quadIndexes, nodeTable, new DatasetControlNone()) ;
        return quadTable;
	}
	
	private static boolean isExistingReadableDirectory(String directory) {
		File path = new File(directory);
		return path.exists() && path.isDirectory() && path.canRead();
	}

	private static boolean isNonExistingFileInWritableDirectory(String filename) {
		File file = new File(filename);
		File path = file.getParentFile();
		return path.exists() && path.isDirectory() && (!file.exists()) && path.canWrite();
	}

	private static void usage() {
		System.out.println("tdb2al <location> <filename> [uri]\n");
		System.out.println("  <location> ......................... a directory containing TDB indexes");
		System.out.println("  <filename> ....... a non existing file where to save the adjacency list");
		System.out.println("  [uri] ....... optional, URI. Default is http://xmlns.com/foaf/0.1/knows\n");
		System.exit(1);
	}

}
