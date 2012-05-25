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

package org.apache.jena.grande.giraph.sssps;

import java.io.IOException;
import java.util.Iterator;

import org.apache.giraph.examples.IntIntNullIntTextInputFormat;
import org.apache.giraph.graph.EdgeListVertex;
import org.apache.giraph.graph.GiraphJob;
import org.apache.giraph.lib.IdWithValueTextOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.jena.grande.Constants;
import org.apache.jena.grande.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class SingleSourceShortestPaths extends EdgeListVertex<IntWritable, IntWritable, NullWritable, IntWritable> implements Tool {

	private static final Logger log = LoggerFactory.getLogger(SingleSourceShortestPaths.class);
	
	public static final String SOURCE_VERTEX = "giraph.example.source";
	public static final int SOURCE_VERTEX_DEFAULT = 3;

	
	@Override
	public int run(String[] args) throws Exception {
		log.debug("run({})", Utils.toString(args));
		Preconditions.checkArgument(args.length == 4, "run: Must have 4 arguments <input path> <output path> <source vertex> <# of workers>");

		Configuration configuration = getConf();
        boolean overrideOutput = configuration.getBoolean(Constants.OPTION_OVERWRITE_OUTPUT, Constants.OPTION_OVERWRITE_OUTPUT_DEFAULT);
        FileSystem fs = FileSystem.get(new Path(args[1]).toUri(), configuration);
        if ( overrideOutput ) {
            fs.delete(new Path(args[1]), true);
        }

		GiraphJob job = new GiraphJob(getConf(), getClass().getName());
		job.setVertexClass(getClass());
		job.setVertexInputFormatClass(IntIntNullIntTextInputFormat.class);
		job.setVertexOutputFormatClass(IdWithValueTextOutputFormat.class);
		FileInputFormat.addInputPath(job.getInternalJob(), new Path(args[0]));
		FileOutputFormat.setOutputPath(job.getInternalJob(), new Path(args[1]));
		job.getConfiguration().set(SOURCE_VERTEX, args[2]);
		job.setWorkerConfiguration(Integer.parseInt(args[3]), Integer.parseInt(args[3]), 100.0f);
		return job.run(true) ? 0 : -1;
	}

	@Override
	public void compute(Iterator<IntWritable> msgIterator) throws IOException {
		log.debug("compute(...)::{}#{} ...", getVertexId(), getSuperstep());
	    if ( ( getSuperstep() == 0 ) || ( getSuperstep() == 1 ) ) {
	    	setVertexValue(new IntWritable(Integer.MAX_VALUE));
	    }
	    int minDist = isSource() ? 0 : Integer.MAX_VALUE;
	    log.debug("compute(...)::{}#{}: min = {}, value = {}", new Object[]{getVertexId(), getSuperstep(), minDist, getVertexValue()});
	    while (msgIterator.hasNext()) {
	    	IntWritable msg = msgIterator.next();
	    	log.debug("compute(...)::{}#{}: <--[{}]-- from ?", new Object[]{getVertexId(), getSuperstep(), msg});
	        minDist = Math.min(minDist, msg.get());
		    log.debug("compute(...)::{}#{}: min = {}", new Object[]{getVertexId(), getSuperstep(), minDist});
	    }
	    if (minDist < getVertexValue().get()) {
	        setVertexValue(new IntWritable(minDist));
		    log.debug("compute(...)::{}#{}: value = {}", new Object[]{getVertexId(), getSuperstep(), getVertexValue()});
	        for (IntWritable targetVertexId : this) {
	    	    log.debug("compute(...)::{}#{}: {} --[{}]--> {}", new Object[]{getVertexId(), getSuperstep(), getVertexId(), minDist+1, targetVertexId});
	        	sendMsg(targetVertexId, new IntWritable(minDist + 1));
	        }
	    }
	    voteToHalt();
	}

	private boolean isSource() {
		boolean result = getVertexId().get() == getConf().getInt(SOURCE_VERTEX, SOURCE_VERTEX_DEFAULT);
		log.debug("isSource() --> {}", result);
		return result;
	}

	public static void main(String[] args) throws Exception {
		log.debug("main({})", Utils.toString(args));
		System.exit(ToolRunner.run(new SingleSourceShortestPaths(), args));
	}
	
}
