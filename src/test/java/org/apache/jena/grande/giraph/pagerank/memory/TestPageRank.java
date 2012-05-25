package org.apache.jena.grande.giraph.pagerank.memory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import junit.framework.TestCase;

public class TestPageRank extends TestCase {

	public void testPageRank() throws IOException {
		File input = new File ("src/test/resources/pagerank.txt");
		BufferedReader in = new BufferedReader(new FileReader (input)) ;

		PlainPageRank pagerank1 = new PlainPageRank (in, 0.85d, 30) ;
		Map<Node, Double> result1 = pagerank1.compute() ;
		
		JungPageRank pagerank2 = new JungPageRank(input, 30, 0.0000001d, 0.15d);
		Map<Node, Double> result2 = pagerank2.compute();

		assertEquals (result1.keySet().size(), result2.keySet().size() );
		for (Node node : result1.keySet()) {
			assertTrue( result2.containsKey(node) );
			assertEquals ( 0.000001d, result1.get(node), result2.get(node) );
		}
		
		print(result1);
	}
	
	private void print(Map<Node, Double> result) {
		Map<Node, Double> sorted = sortByValue(result);
		for (Node node : sorted.keySet()) {
			System.out.print(String.format("%10s : %1.20f\n", node.getId(), sorted.get(node)));
		}
	}
	
	private Map<Node, Double> sortByValue(Map<Node, Double> map) {
		List<Map.Entry<Node, Double>> list = new LinkedList<Map.Entry<Node, Double>>(map.entrySet());

		Collections.sort(list, new Comparator<Map.Entry<Node, Double>>() {
			@Override
			public int compare(Entry<Node, Double> o1, Entry<Node, Double> o2) {
				return - ((Map.Entry<Node, Double>) (o1)).getValue().compareTo(((Map.Entry<Node, Double>) (o2)).getValue());
			}
		});

	    Map<Node, Double> result = new LinkedHashMap<Node, Double>();
	    for (Iterator<Entry<Node, Double>> it = list.iterator(); it.hasNext();) {
	        Map.Entry<Node, Double> entry = it.next();
	        result.put(entry.getKey(), entry.getValue());
	    }

	    return result;
	} 
	

}