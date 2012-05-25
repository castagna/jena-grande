package org.apache.jena.grande.giraph.pagerank.memory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import junit.framework.TestCase;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.jena.grande.giraph.pagerank.RunPageRankVertexLocally;

public class TestPageRank extends TestCase {

	private static final String filename = "src/test/resources/pagerank.txt"; 
	
	public void testPlainPageRank() throws IOException {
		File input = new File (filename);
		BufferedReader in = new BufferedReader(new FileReader (input)) ;

		PlainPageRank pagerank1 = new PlainPageRank (in, 0.85d, 30) ;
		Map<String, Double> result1 = pagerank1.compute() ;
		
		JungPageRank pagerank2 = new JungPageRank(input, 30, 0.0000001d, 0.15d);
		Map<String, Double> result2 = pagerank2.compute();

		check ( result1, result2 );
	}

	public void testPageRankVertex() throws Exception {
		File input = new File (filename);
		JungPageRank pagerank1 = new JungPageRank(input, 30, 0.0000001d, 0.15d);
		Map<String, Double> result1 = pagerank1.compute();

		Map<String, Double> result2 = RunPageRankVertexLocally.run(filename);

		check ( result1, result2 );
	}
	
	private void check ( Map<String, Double> result1, Map<String, Double> result2 ) throws IOException {
		assertEquals ( result1.keySet().size(), result2.keySet().size() );
		
		// check order
		String[] array1 = sortByValue(result1).keySet().toArray(new String[]{});
		String[] array2 = sortByValue(result2).keySet().toArray(new String[]{});
		for ( int i = 0; i < array1.length; i++ ) {
			assertEquals ( array1[i], array2[i] );
		}

		// pagerank values should be a probability distribution, right? Sum should be 1 then.
		assertEquals ( dump(result1, result2), 1.0, sum(result1), 0.0001 );
		assertEquals ( dump(result1, result2), 1.0, sum(result2), 0.0001 );

		// check actual pagerank values
		for ( String key : result1.keySet() ) {
			assertTrue( result2.containsKey(key) );
			assertEquals ( dump(result1, result2), result1.get(key), result2.get(key), 0.000001d );
		}
	}

	private double sum ( Map<?, Double> result ) {
		double sum = 0;
		for ( Object key : result.keySet() ) {
			sum += result.get(key);
		}
		return sum;
	}
	
	private String dump ( Map<?, Double> result1, Map<?, Double> result2 ) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		baos.write("Expected:\n".getBytes());
		print ( baos, result1 );
		baos.write("Actual:\n".getBytes());
		print ( baos, result2 );
		return baos.toString();
	}
	
	private void print ( OutputStream out, Map<?, Double> result ) throws UnsupportedEncodingException, IOException {
		Map<?, Double> sorted = sortByValue(result);
		double sum = 0;
		for (Object node : sorted.keySet()) {
			out.write(String.format("%10s : %1.20f\n", node, sorted.get(node)).getBytes());
			sum += sorted.get(node);
		}
		out.write (String.format("sum = %1.20f\n", sum).getBytes());
	}
	
	private Map<?, Double> sortByValue(Map<?, Double> map) {
		List<Map.Entry<?, Double>> list = new LinkedList<Map.Entry<?, Double>>(map.entrySet());

		Collections.sort(list, new Comparator<Map.Entry<?, Double>>() {
			@Override
			public int compare(Entry<?, Double> o1, Entry<?, Double> o2) {
				return - ((Map.Entry<?, Double>) (o1)).getValue().compareTo(((Map.Entry<?, Double>) (o2)).getValue());
			}
		});

	    Map<Object, Double> result = new LinkedHashMap<Object, Double>();
	    for (Iterator<Entry<?, Double>> it = list.iterator(); it.hasNext();) {
	        Map.Entry<?, Double> entry = it.next();
	        result.put(entry.getKey(), entry.getValue());
	    }

	    return result;
	} 
	

}