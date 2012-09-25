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

package org.apache.jena.grande.giraph;

import java.io.IOException;

import org.apache.giraph.GiraphConfiguration;
import org.apache.giraph.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.Edge;
import org.apache.giraph.graph.EdgeListVertex;
import org.apache.giraph.graph.GiraphJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.jena.grande.Constants;
import org.apache.jena.grande.Utils;
import org.apache.jena.grande.mapreduce.io.NodeWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class FoafShortestPathsVertex extends EdgeListVertex<NodeWritable, IntWritable, NodeWritable, IntWritable> implements Tool {

	private static final Logger log = LoggerFactory.getLogger(FoafShortestPathsVertex.class);

	public static final String SOURCE_URI = "FoafShortestPathsVertex.sourceURI";
	public static final String SOURCE_URI_DEFAULT = "http://example.org/alice";

	private boolean isSource() {
		boolean result = getId().getNode().getURI().equals(getContext().getConfiguration().get(SOURCE_URI, SOURCE_URI_DEFAULT));
		log.debug("isSource() --> {}", result);
		return result;
	}

	@Override
	public void compute(Iterable<IntWritable> msgIterator) throws IOException {
		log.debug("compute(...)::{}#{} ...", getId(), getSuperstep());
	    if ( ( getSuperstep() == 0 ) || ( getSuperstep() == 1 ) ) {
	    	setValue(new IntWritable(Integer.MAX_VALUE));
	    }
	    int minDist = isSource() ? 0 : Integer.MAX_VALUE;
	    log.debug("compute(...)::{}#{}: min = {}, value = {}", new Object[]{getId(), getSuperstep(), minDist, getValue()});
	    for ( IntWritable msg : msgIterator ) {
	    	log.debug("compute(...)::{}#{}: <--[{}]-- from ?", new Object[]{getId(), getSuperstep(), msg});
	        minDist = Math.min(minDist, msg.get());
		    log.debug("compute(...)::{}#{}: min = {}", new Object[]{getId(), getSuperstep(), minDist});
	    }
	    if (minDist < getValue().get()) {
	        setValue(new IntWritable(minDist));
		    log.debug("compute(...)::{}#{}: value = {}", new Object[]{getId(), getSuperstep(), getValue()});
	        for (Edge<NodeWritable, NodeWritable> edge : getEdges()) {
	    	    log.debug("compute(...)::{}#{}: {} --[{}]--> {}", new Object[]{getId(), getSuperstep(), getId(), minDist+1, edge.getTargetVertexId()});
	        	sendMessage(edge.getTargetVertexId(), new IntWritable(minDist + 1));
	        }
	    }
	    voteToHalt();
	}
	
	@Override
	public int run(String[] args) throws Exception {
		log.debug("run({})", Utils.toString(args));
		Preconditions.checkArgument(args.length == 4, "run: Must have 4 arguments <input path> <output path> <source vertex uri> <# of workers>");

		Configuration configuration = getConf();
        boolean overrideOutput = configuration.getBoolean(Constants.OPTION_OVERWRITE_OUTPUT, Constants.OPTION_OVERWRITE_OUTPUT_DEFAULT);
        FileSystem fs = FileSystem.get(new Path(args[1]).toUri(), configuration);
        if ( overrideOutput ) {
            fs.delete(new Path(args[1]), true);
        }

        GiraphConfiguration giraphConfiguration = new GiraphConfiguration(getConf());
        giraphConfiguration.setVertexClass(getClass());
        giraphConfiguration.setVertexInputFormatClass(TurtleVertexInputFormat.class);
		giraphConfiguration.setVertexOutputFormatClass(TurtleVertexOutputFormat.class);
		giraphConfiguration.set(SOURCE_URI, args[2]);
		giraphConfiguration.setWorkerConfiguration(Integer.parseInt(args[3]), Integer.parseInt(args[3]), 100.0f);
        
		GiraphJob job = new GiraphJob(getConf(), getClass().getName());
		FileInputFormat.addInputPath(job.getInternalJob(), new Path(args[0]));
		FileOutputFormat.setOutputPath(job.getInternalJob(), new Path(args[1]));
		return job.run(true) ? 0 : -1;
	}

	@Override
	public void setConf(Configuration conf) {
		super.setConf(new ImmutableClassesGiraphConfiguration<NodeWritable, IntWritable, NodeWritable, IntWritable>(conf));
	}

	public static void main(String[] args) throws Exception {
		log.debug("main({})", Utils.toString(args));
		System.exit(ToolRunner.run(new FoafShortestPathsVertex(), args));
	}
	
}
