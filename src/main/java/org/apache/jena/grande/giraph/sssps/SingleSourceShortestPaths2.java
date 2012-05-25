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
import java.util.Arrays;
import java.util.Iterator;

import org.apache.giraph.graph.EdgeListVertex;
import org.apache.giraph.graph.GiraphJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.jena.grande.Constants;
import org.apache.jena.grande.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import dev.MyIdWithValueTextOutputFormat;

public class SingleSourceShortestPaths2 extends EdgeListVertex<Text, Text, NullWritable, Text> implements Tool {

	private static final Logger log = LoggerFactory.getLogger(SingleSourceShortestPaths2.class);
	
	public static final String SOURCE_VERTEX = "giraph.example.source";
	public static final String SOURCE_VERTEX_DEFAULT = "3";

	
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
		job.setVertexInputFormatClass(TextTextNullTextTextVertexInputFormat.class);
		job.setVertexOutputFormatClass(MyIdWithValueTextOutputFormat.class);
		FileInputFormat.addInputPath(job.getInternalJob(), new Path(args[0]));
		FileOutputFormat.setOutputPath(job.getInternalJob(), new Path(args[1]));
		job.getConfiguration().set(SOURCE_VERTEX, args[2]);
		job.setWorkerConfiguration(Integer.parseInt(args[3]), Integer.parseInt(args[3]), 100.0f);
		return job.run(true) ? 0 : -1;
	}

	@Override
	public void compute(Iterator<Text> msgIterator) throws IOException {
		try {
			log.debug("{}#{} - compute(...)", new Object[]{getVertexId(), getSuperstep()});
			if ( getSuperstep() == 0 ) {
				setDistance(Integer.MAX_VALUE);
		    }		    
		    int minDist = isSource() ? 0 : Integer.MAX_VALUE;
			log.debug("{}#{} - compute(...) < min = {}, value = {}", new Object[]{getVertexId(), getSuperstep(), minDist, getVertexValue()});
		    Text minMsg = null;
		    while (msgIterator.hasNext()) {
		    	Text msg = msgIterator.next();
		    	log.debug("{}#{} - compute(...): <--[{}]-- from ?", new Object[]{getVertexId(), getSuperstep(), msg});
		    	int msgDist = getDistance(msg);
		    	if ( msgDist < minDist ) {
		    		minDist = msgDist;
		    		minMsg = msg;
		    	}
		    }
		    if ( minDist < getDistance(getVertexValue()) ) {
		        Text msg = getMessage(minDist + 1, minMsg);
		        if ( minMsg != null ) { 
		        	setVertexValue(minMsg);
		        } else {
		        	setVertexValue(new Text("0"));
		        }
		        for (Text targetVertexId : this) {
		    	    log.debug("{}#{} - compute(...): {} --[{}]--> {}", new Object[]{getVertexId(), getSuperstep(), getVertexId(), msg, targetVertexId});
		        	sendMsg(targetVertexId, msg);
		        }
		    }
		    log.debug("{}#{} - compute(...) > min = {}, value = {}", new Object[]{getVertexId(), getSuperstep(), minDist, getVertexValue()});
		    voteToHalt();
		} catch (Throwable e) {
			log.debug(e.getMessage(), e);
			System.exit(1);
		}
	}

	private void setDistance(int distance) {
		log.debug("{}#{} - setDistance({})", new Object[]{getVertexId(), getSuperstep(), distance});

		StringBuilder sb = new StringBuilder();
		sb.append(distance);

		Text value = getVertexValue();
		if ( value != null ) {
			String[] tokens = value.toString().split(" ");
			for ( int i = 1; i < tokens.length; i++ ) {
				sb.append(" ");
				sb.append(tokens[i]);
			}			
		}
		
		setVertexValue(new Text(sb.toString()));
	}

	private int getDistance(Text msg) {
		if ( msg == null ) return Integer.MAX_VALUE;
		String[] tokens = msg.toString().split(" ");
		int result = Integer.parseInt(tokens[0]);
		log.debug("{}#{} - getDistance({}) --> {}", new Object[]{getVertexId(), getSuperstep(), msg, result});
		return result;
	}
	
	private Text getMessage(int distance, Text msg) {
		String[] path = new String[]{};
		if ( msg != null ) {
			String[] tokens = msg.toString().split(" ");
			path = Arrays.copyOfRange(tokens, 1, tokens.length);			
		}
		StringBuilder sb = new StringBuilder();
		sb.append(distance);
		for ( String str : path ) {
			sb.append(" ");
			sb.append(str);
		}
		sb.append(" ");
		sb.append(getVertexId().toString());
		Text result = new Text(sb.toString());
		log.debug("{}#{} - getMessage({}, {}) --> {}", new Object[]{getVertexId(), getSuperstep(), distance, msg, result});
		return result;
	}

	private boolean isSource() {
		boolean result = getVertexId().toString().equals ( getConf().get(SOURCE_VERTEX, SOURCE_VERTEX_DEFAULT) );
		log.debug("{}#{} - isSource() --> {}", new Object[]{getVertexId(), getSuperstep(), result});
		return result;
	}

	public static void main(String[] args) throws Exception {
		log.debug("main({})", Utils.toString(args));
		System.exit(ToolRunner.run(new SingleSourceShortestPaths2(), args));
	}
	
}
