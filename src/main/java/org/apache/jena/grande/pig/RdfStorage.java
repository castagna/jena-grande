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

package org.apache.jena.grande.pig;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.jena.grande.NodeEncoder;
import org.apache.jena.grande.mapreduce.io.QuadWritable;
import org.apache.pig.FileInputLoadFunc;
import org.apache.pig.LoadFunc;
import org.apache.pig.ResourceSchema;
import org.apache.pig.StoreFunc;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.Quad;

public class RdfStorage extends FileInputLoadFunc implements StoreFuncInterface {

	private static final Logger log = LoggerFactory.getLogger(RdfStorage.class);
	
    private String location;
    private RecordReader<LongWritable, QuadWritable> reader;
    private RecordWriter<NullWritable, QuadWritable> writer;

    @Override
    public void setLocation(String location, Job job) throws IOException {
    	log.debug("setLocation({}, {})", location, job);
        this.location = location;
        FileInputFormat.setInputPaths(job, location);
    }
    
	@Override
	public InputFormat<LongWritable, QuadWritable> getInputFormat() throws IOException {
		InputFormat<LongWritable, QuadWritable> inputFormat = new NQuadsPigInputFormat(); 
        log.debug("getInputFormat() --> {}", inputFormat);
        return inputFormat;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public void prepareToRead(@SuppressWarnings("rawtypes") RecordReader reader, PigSplit split) throws IOException {
		log.debug("prepareToRead({}, {})", reader, split);
		this.reader = reader;
	}

	private final TupleFactory tupleFactory = TupleFactory.getInstance();
	
	@Override
	public Tuple getNext() throws IOException {
		Tuple tuple = null;
        try {
    		if ( reader.nextKeyValue() ) {
                QuadWritable quad = reader.getCurrentValue();
                tuple = tupleFactory.newTuple(4);
                tuple.set(0, NodeEncoder.asString(quad.getQuad().getGraph()));
                tuple.set(1, NodeEncoder.asString(quad.getQuad().getSubject()));
                tuple.set(2, NodeEncoder.asString(quad.getQuad().getPredicate()));
                tuple.set(3, NodeEncoder.asString(quad.getQuad().getObject()));
    		}
        } catch (InterruptedException e) {
            throw new IOException(String.format("Error while reading %s", location));
        }
        log.debug("getNext() --> {}", tuple);
        return tuple;
	}

	@Override
	public String relToAbsPathForStoreLocation(String location, Path curDir) throws IOException {
		String path = LoadFunc.getAbsolutePath(location, curDir);
		log.debug("relToAbsPathForStoreLocation({}, {}) --> {}", new Object[]{location, curDir, path});
		return path;
	}

	@Override
	public OutputFormat<NullWritable, QuadWritable> getOutputFormat() throws IOException {
		OutputFormat<NullWritable, QuadWritable> outputFormat = new NQuadsPigOutputFormat();
		log.debug("getOutputFormat() --> {}", outputFormat);
		return outputFormat;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void setStoreLocation(String location, Job job) throws IOException {
		log.debug("setStoreLocation({}, {})", location, job);
        job.getConfiguration().set("mapred.textoutputformat.separator", "");
        FileOutputFormat.setOutputPath(job, new Path(location));
        if( "true".equals( job.getConfiguration().get( "output.compression.enabled" ) ) ) {
            FileOutputFormat.setCompressOutput( job, true );
            String codec = job.getConfiguration().get( "output.compression.codec" );
            try {
                FileOutputFormat.setOutputCompressorClass( job,  (Class<? extends CompressionCodec>) Class.forName( codec ) );
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("Class not found: " + codec );
            }
        } else {
            if (location.endsWith(".bz2") || location.endsWith(".bz")) {
                FileOutputFormat.setCompressOutput(job, true);
                FileOutputFormat.setOutputCompressorClass(job,  BZip2Codec.class);
            }  else if (location.endsWith(".gz")) {
                FileOutputFormat.setCompressOutput(job, true);
                FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
            } else {
                FileOutputFormat.setCompressOutput( job, false);
            }
        }
	}

	@Override
	public void checkSchema(ResourceSchema schema) throws IOException {
		log.debug("checkSchema({})", schema);
		// no-op
	}

	@SuppressWarnings("unchecked")
	@Override
	public void prepareToWrite(@SuppressWarnings("rawtypes") RecordWriter writer) throws IOException {
		log.debug("prepareToWrite({})", writer);
		this.writer = writer;
	}

	@Override
	public void putNext(Tuple tuple) throws IOException {
		log.debug("putNext({})", tuple);
        try {
        	Node g = NodeEncoder.asNode((String)tuple.get(0));
        	Node s = NodeEncoder.asNode((String)tuple.get(1));
        	Node p = NodeEncoder.asNode((String)tuple.get(2));
        	Node o = NodeEncoder.asNode((String)tuple.get(3));
        	Quad quad = new Quad(g,s,p,o);
        	QuadWritable quadWritable = new QuadWritable(quad);
            writer.write(NullWritable.get(), quadWritable);            
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
	}

	@Override
	public void setStoreFuncUDFContextSignature(String signature) {
		log.debug("setStoreFuncUDFContextSignature({})", signature);
		// no-op
	}

	@Override
	public void cleanupOnFailure(String location, Job job) throws IOException {
		log.debug("cleanupOnFailure({}, {})", location, job);
		StoreFunc.cleanupOnFailureImpl(location, job);
	}

}
