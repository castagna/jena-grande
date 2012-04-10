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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.pig.LoadFunc;
import org.apache.pig.ResourceSchema;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class RdfStorage extends LoadFunc implements StoreFuncInterface {

	private Job job;
    private String location;
    private RecordReader<?,?> reader;

    @Override
    public void setLocation(String location, Job job) throws IOException {
        this.location = location;
        this.job = job;
    }
    
	@Override
	public InputFormat<?, ?> getInputFormat() throws IOException {
        if ( location.endsWith(".nq") ) {
            return new NQuadsPigInputFormat();
        } else if ( location.endsWith(".nt") ) {
            return new NTriplesPigInputFormat();        	
        } else {
            return new NQuadsPigInputFormat();
        }
	}
	
	@Override
	public void prepareToRead(@SuppressWarnings("rawtypes") RecordReader reader, PigSplit split) throws IOException {
		this.reader = reader;
	}

	private final TupleFactory tupleFactory = TupleFactory.getInstance();
	
	@Override
	public Tuple getNext() throws IOException {
        try {
    		if ( reader.nextKeyValue() ) {
                Text value = (Text) reader.getCurrentValue();
                byte[] ba = value.getBytes();
                return tupleFactory.newTuple(new DataByteArray(ba, 0, value.getLength()));
    		} else {
    			return null;
    		}
        } catch (InterruptedException e) {
            throw new IOException(String.format("Error while reading %s", location));
        }
	}

	@Override
	public String relToAbsPathForStoreLocation(String location, Path curDir)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public OutputFormat getOutputFormat() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setStoreLocation(String location, Job job) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void checkSchema(ResourceSchema s) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void prepareToWrite(RecordWriter writer) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void putNext(Tuple t) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setStoreFuncUDFContextSignature(String signature) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void cleanupOnFailure(String location, Job job) throws IOException {
		// TODO Auto-generated method stub
		
	}

}
