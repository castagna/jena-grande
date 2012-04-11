package org.apache.jena.grande.examples;

import java.io.IOException;

import org.apache.pig.PigServer;

public class RunPig {
	
	private static final String output = "./target/output";

	public static void main(String[] args) throws IOException {
		PigServer pig = new PigServer("local");
		pig.deleteFile(output);
		// pig.debugOn();
		// pig.registerJar("./target/jena-grande-0.1-SNAPSHOT.jar");
		pig.registerQuery("quads = LOAD './src/test/resources/data.nq' USING org.apache.jena.grande.pig.RdfStorage() AS (g,s,p,o);");
		pig.registerQuery("a = FILTER quads BY ( p == '<http://xmlns.com/foaf/0.1/name>' ) ;");
		pig.registerQuery("b = FILTER a BY ( o == '\"Bob\"' ) ;");
		pig.store("b", output, "org.apache.jena.grande.pig.RdfStorage()");
	}

}
