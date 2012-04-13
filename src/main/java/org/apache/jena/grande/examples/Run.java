package org.apache.jena.grande.examples;

import org.apache.jena.grande.NodeEncoder;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.util.NodeFactory;

public class Run {

	public static void main(String[] args) {
		Node node = NodeFactory.parseNode("\"\"\"This\nis\na\ntest.\"\"\"");
		System.out.println(node.getLiteralLexicalForm());
		System.out.println(NodeEncoder.asString(node));
	}

}
