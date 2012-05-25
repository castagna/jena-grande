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

package org.apache.jena.grande.mapreduce.io;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;
import org.apache.jena.grande.Utils;
import org.openjena.riot.lang.LangNQuads;
import org.openjena.riot.system.ParserProfile;
import org.openjena.riot.tokens.Tokenizer;
import org.openjena.riot.tokens.TokenizerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QuadRecordReader extends RecordReader<LongWritable, QuadWritable> {

    private static final Logger log = LoggerFactory.getLogger(QuadRecordReader.class);

    public static final String MAX_LINE_LENGTH = "mapreduce.input.linerecordreader.line.maxlength";

    private CompressionCodecFactory compressionCodecs = null;
    private long start;
    private long pos;
    private long end;
    private LineReader in;
    private int maxLineLength;
    private LongWritable key = null;
    private Text value = null;
    private QuadWritable quad = null;
    private ParserProfile profile = null;
    
	@Override
    public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException, InterruptedException {
		log.debug("initialize({}, {})", genericSplit, context);

        FileSplit split = (FileSplit) genericSplit;
        profile = Utils.createParserProfile(context, split.getPath()); // RIOT configuration
        Configuration job = context.getConfiguration();
        this.maxLineLength = job.getInt(MAX_LINE_LENGTH, Integer.MAX_VALUE);
        start = split.getStart();
        end = start + split.getLength();
        final Path file = split.getPath();
        compressionCodecs = new CompressionCodecFactory(job);
        final CompressionCodec codec = compressionCodecs.getCodec(file);

        // open the file and seek to the start of the split
        FileSystem fs = file.getFileSystem(job);
        FSDataInputStream fileIn = fs.open(file);
        boolean skipFirstLine = false;
        if (codec != null) {
            in = new LineReader(codec.createInputStream(fileIn), job);
            end = Long.MAX_VALUE;
        } else {
            if (start != 0) {
                skipFirstLine = true;
                --start;
                fileIn.seek(start);
            }
            in = new LineReader(fileIn, job);
        }
        if (skipFirstLine) {  // skip first line and re-establish "start".
            start += in.readLine(new Text(), 0, (int)Math.min((long)Integer.MAX_VALUE, end - start));
        }
        this.pos = start;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (key == null) {
            key = new LongWritable();
        }
        key.set(pos);
        if (value == null) {
            value = new Text();
            quad = null;
        }
        int newSize = 0;
        while (pos < end) {
            newSize = in.readLine(value, maxLineLength, Math.max((int)Math.min(Integer.MAX_VALUE, end-pos), maxLineLength));
            Tokenizer tokenizer = TokenizerFactory.makeTokenizerASCII(value.toString()) ;
            LangNQuads parser = new LangNQuads(tokenizer, profile, null) ;
            if ( parser.hasNext() ) {
                quad = new QuadWritable(parser.next());
            }
            if (newSize == 0) {
                break;
            }
            pos += newSize;
            if (newSize < maxLineLength) {
                break;
            }
            log.info("Skipped line of size " + newSize + " at pos " + (pos - newSize));
        }
        boolean result = true;
        if (newSize == 0) {
            key = null;
            value = null;
            quad = null;
            result = false;
        }
        log.debug("nextKeyValue() --> {}", result); 
        return result;
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
    	log.debug("getCurrentKey() --> {}", key);
        return key;
    }

    @Override
    public QuadWritable getCurrentValue() throws IOException, InterruptedException {
    	log.debug("getCurrentValue() --> {}", quad);
        return quad;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
    	float progress = 0.0f;
        if (start != end) {
            progress = Math.min(1.0f, (pos - start) / (float) (end - start));
        }
        log.debug("getProgress() --> {}", progress);
        return progress;
    }

    @Override
    public void close() throws IOException {
    	log.debug("close()");
        if (in != null) {
            in.close(); 
        }
    }
    
}
