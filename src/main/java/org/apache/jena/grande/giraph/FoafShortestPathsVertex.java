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

package org.apache.jena.grande.giraph;

import java.io.IOException;
import java.util.Iterator;

import org.apache.giraph.graph.EdgeListVertex;
import org.apache.giraph.graph.GiraphJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.jena.grande.Utils;
import org.apache.jena.grande.mapreduce.io.NodeWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class FoafShortestPathsVertex  extends EdgeListVertex<NodeWritable, IntWritable, NodeWritable, IntWritable> implements Tool {

	private static final Logger log = LoggerFactory.getLogger(FoafShortestPathsVertex.class);

	public static final String SOURCE_URI = "FoafShortestPathsVertex.sourceURI";
	public static final String SOURCE_URI_DEFAULT = "...";
	private Configuration conf;

	private boolean isSource() {
		boolean result = getVertexId().getNode().getURI().equals(getContext().getConfiguration().get(SOURCE_URI, SOURCE_URI_DEFAULT));
		log.debug("isSource() --> {}", result);
		return result;
	}

	@Override
	public void compute(Iterator<IntWritable> msgIterator) throws IOException {
	    if (getSuperstep() == 0) {
	    	setVertexValue(new IntWritable(Integer.MAX_VALUE));
	    }
	    int minDist = isSource() ? 0 : Integer.MAX_VALUE;
	    while (msgIterator.hasNext()) {
	        minDist = Math.min(minDist, msgIterator.next().get());
	    }
	    if ( log.isDebugEnabled() ) {
	        log.debug("Vertex " + getVertexId() + " got minDist = " + minDist + " vertex value = " + getVertexValue());
	    }
	    if (minDist < getVertexValue().get()) {
	        setVertexValue(new IntWritable(minDist));
	        for (NodeWritable targetVertexId : this) {
	        	if ( log.isDebugEnabled() ) {
	                log.debug("Vertex " + getVertexId() + " sent to " + targetVertexId + " = " + (minDist + 1));
	        	}
	        	sendMsg(targetVertexId, new IntWritable(minDist + 1));
	        }
	    }
	    voteToHalt();
	}

	@Override
	public Configuration getConf() {
		log.debug("getConf() --> {}", conf);
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		log.debug("setConf({})", conf);
		this.conf = conf;
	}
	
	@Override
	public int run(String[] args) throws Exception {
		log.debug("run({})", Utils.toString(args));
		Preconditions.checkArgument(args.length == 4, "run: Must have 4 arguments <input path> <output path> <source vertex uri> <# of workers>");
		GiraphJob job = new GiraphJob(getConf(), getClass().getName());
		job.setVertexClass(getClass());
		job.setVertexInputFormatClass(TurtleVertexInputFormat.class);
		job.setVertexOutputFormatClass(TurtleVertexOutputFormat.class);
		FileInputFormat.addInputPath(job.getInternalJob(), new Path(args[0]));
		FileOutputFormat.setOutputPath(job.getInternalJob(), new Path(args[1]));
		job.getConfiguration().set(SOURCE_URI, args[2]);
		job.setWorkerConfiguration(Integer.parseInt(args[3]), Integer.parseInt(args[3]), 100.0f);
		return job.run(true) ? 0 : -1;
	}

	public static void main(String[] args) throws Exception {
		log.debug("main({})", Utils.toString(args));
		System.exit(ToolRunner.run(new FoafShortestPathsVertex(), args));
	}
	
}
