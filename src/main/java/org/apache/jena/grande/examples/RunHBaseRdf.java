package org.apache.jena.grande.examples;

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
	private final static String filename = "src/test/resources/data.nq";
	
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
