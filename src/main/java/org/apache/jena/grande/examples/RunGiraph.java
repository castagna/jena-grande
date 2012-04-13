package org.apache.jena.grande.examples;

import org.apache.giraph.examples.SimpleShortestPathsVertex;
import org.apache.giraph.examples.SimpleShortestPathsVertex.SimpleShortestPathsVertexInputFormat;
import org.apache.giraph.examples.SimpleShortestPathsVertex.SimpleShortestPathsVertexOutputFormat;
import org.apache.giraph.graph.GiraphJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RunGiraph {

	public static void main(String[] args) throws Exception {
	    GiraphJob job = new GiraphJob("shortest paths");
	    Configuration conf = job.getConfiguration();
	    conf.setBoolean(GiraphJob.SPLIT_MASTER_WORKER, false);
	    conf.setBoolean(GiraphJob.LOCAL_TEST_MODE, true);
	    // conf.set(GiraphJob.ZOOKEEPER_JAR, "file://target/dependency/zookeeper-3.3.3.jar");
	    job.setWorkerConfiguration(1, 1, 100.0f);

	    job.setVertexClass(SimpleShortestPathsVertex.class);
	    job.setVertexInputFormatClass(SimpleShortestPathsVertexInputFormat.class);
	    job.setVertexOutputFormatClass(SimpleShortestPathsVertexOutputFormat.class);
	    
	    // input
	    FileInputFormat.addInputPath(job.getInternalJob(), new Path("src/main/resources/giraph1.txt"));
	    
	    // output
	    Path outputPath = new Path("target/giraph1");
	    FileSystem hdfs = FileSystem.get(conf);
	    hdfs.delete(outputPath, true);
	    FileOutputFormat.setOutputPath(job.getInternalJob(), outputPath);

	    job.run(true);
	}

}
