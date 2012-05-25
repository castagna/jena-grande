package org.apache.jena.grande.giraph.pagerank.memory;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.jena.grande.MemoryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PlainPageRank {

	private static final Logger LOG = LoggerFactory.getLogger(PlainPageRank.class);
	
	private Graph graph = new Graph() ;
	private Map<Node, Double> pagerank_current = new HashMap<Node, Double>() ; 
	private Map<Node, Double> pagerank_new = new HashMap<Node, Double>() ; 
	
	private double dumping_factor ;
	private int iterations ;
	
	public PlainPageRank ( BufferedReader in, double dumping_factor, int iterations ) {
		this.dumping_factor = dumping_factor ;
		this.iterations = iterations ;		
		
		try {
			MemoryUtil.printUsedMemory() ;
			load_data ( in ) ;
			MemoryUtil.printUsedMemory() ;
			
			initialize_pagerank() ;
		} catch ( IOException e ) {
			e.printStackTrace();
		}
	}
	
	public Map<Node, Double> compute() {
		double teleport = ( 1.0d - dumping_factor ) / graph.countNodes() ;
		for (int i = 0; i < iterations; i++) {
			LOG.debug("iteration " + i);
			double dangling_nodes = 0.0d ;
			for ( Node node : graph.getNodes() ) {
				if ( graph.countOutgoingLinks(node) == 0 ) {
					dangling_nodes += pagerank_current.get ( node ) ;
				}
			}
			dangling_nodes = ( dumping_factor * dangling_nodes ) / graph.countNodes() ;
			
			for ( Node node : graph.getNodes() ) {
				double r = 0.0d ; 
				for ( Node source : graph.getIncomingLinks(node) ) {
					r += pagerank_current.get ( source ) / graph.countOutgoingLinks ( source ) ; 						
				}
				r = dumping_factor * r + dangling_nodes + teleport ;	
				pagerank_new.put ( node, r ) ;
			}
			for ( Node node : graph.getNodes() ) {
				pagerank_current.put ( node, pagerank_new.get ( node ) ) ;
			}
		}

		return pagerank_current ;
	}
	
	private void initialize_pagerank() {
		Double initial_pagerank = ( 1.0d / graph.countNodes() ) ; 
		for ( Node node : graph.getNodes() ) {
			pagerank_current.put ( node, initial_pagerank ) ;
		}
	}
	
	private void load_data ( BufferedReader in ) throws IOException {
		long start = System.currentTimeMillis() ;
		
		String line;
		while ((line = in.readLine()) != null) {
			StringTokenizer st = new StringTokenizer(line) ;
			Node source = new Node(st.nextToken());
			graph.addNode( source ) ;
			HashSet<String> seen = new HashSet<String>();
			while (st.hasMoreTokens()) {
				String token = st.nextToken() ;
				if ( !seen.contains(token) ) { // no multiple links to the same page
					Node destination = new Node(token) ;
					if ( destination != source ) { // no self-links
						graph.addNode ( destination ) ;
						graph.addLink ( source, destination ) ;					
					}
					seen.add(token) ;
				}
			}
		}
		in.close();
		
		LOG.debug(String.format("Loaded %d nodes and %d links in %d ms", graph.countNodes(), graph.countLinks(), (System.currentTimeMillis()-start)));
	}
	
}