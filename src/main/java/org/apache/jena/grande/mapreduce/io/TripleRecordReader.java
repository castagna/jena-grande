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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;
import org.apache.jena.grande.Utils;
import org.openjena.riot.lang.LangNTriples;
import org.openjena.riot.system.ParserProfile;
import org.openjena.riot.tokens.Tokenizer;
import org.openjena.riot.tokens.TokenizerFactory;

public class TripleRecordReader extends RecordReader<LongWritable, TripleWritable> {

    private static final Log LOG = LogFactory.getLog(TripleRecordReader.class);
    public static final String MAX_LINE_LENGTH = "mapreduce.input.linerecordreader.line.maxlength";

    private LongWritable key = null;
    private Text value = null;
    private TripleWritable triple = null;
    
    private long start;
    private long pos;
    private long end;
    
    private LineReader in;
    private FSDataInputStream fileIn;
    private Seekable filePosition;

    private ParserProfile profile;
    
    private int maxLineLength;
//    private Counter inputByteCounter;

    private CompressionCodecFactory compressionCodecs = null;
    private CompressionCodec codec;
    private Decompressor decompressor;
    
//    @SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
    public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException, InterruptedException {
        FileSplit split = (FileSplit) genericSplit;
        
        // RIOT configuration 
        profile = Utils.createParserProfile(context, split.getPath());
        
//        inputByteCounter = ((MapContext)context).getCounter(FileInputFormat.Counter.BYTES_READ);
        Configuration job = context.getConfiguration();
        this.maxLineLength = job.getInt(MAX_LINE_LENGTH, Integer.MAX_VALUE);
        start = split.getStart();
        end = start + split.getLength();
        final Path file = split.getPath();
        compressionCodecs = new CompressionCodecFactory(job);
        codec = compressionCodecs.getCodec(file);

        // open the file and seek to the start of the split
        final FileSystem fs = file.getFileSystem(job);
        fileIn = fs.open(file);
        if (isCompressedInput()) {
            decompressor = CodecPool.getDecompressor(codec);
            in = new LineReader(codec.createInputStream(fileIn, decompressor), job);
            filePosition = fileIn;
        } else {
            fileIn.seek(start);
            in = new LineReader(fileIn, job);
            filePosition = fileIn;
        }
        // If this is not the first split, we always throw away first record
        // because we always (except the last split) read one extra line in
        // next() method.
        if (start != 0) {
            start += in.readLine(new Text(), 0, maxBytesToConsume(start));
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
            triple = null;
        }
        int newSize = 0;
        // We always read one extra line, which lies outside the upper split limit i.e. (end - 1)
        while (getFilePosition() <= end) {
            newSize = in.readLine(value, maxLineLength, Math.max(maxBytesToConsume(pos), maxLineLength));
            Tokenizer tokenizer = TokenizerFactory.makeTokenizerASCII(value.toString()) ;
            LangNTriples parser = new LangNTriples(tokenizer, profile, null) ;
            if ( parser.hasNext() ) {
                triple = new TripleWritable(parser.next());
            }
            if (newSize == 0) {
                break;
            }
            pos += newSize;
//            inputByteCounter.increment(newSize);
            if (newSize < maxLineLength) {
                break;
            }
            LOG.info("Skipped line of size " + newSize + " at pos " + (pos - newSize));
        }
        if (newSize == 0) {
            key = null;
            value = null;
            triple = null;
            return false;
        } else {
            return true;
        }
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public TripleWritable getCurrentValue() throws IOException, InterruptedException {
        return triple;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (getFilePosition() - start) / (float) (end - start));
        }
    }

    @Override
    public void close() throws IOException {
        try {
            if (in != null) {
                in.close();
            }
        } finally {
            if (decompressor != null) {
                CodecPool.returnDecompressor(decompressor);
            }
        }
    }
    
    private boolean isCompressedInput() {
        return (codec != null);
    }
    
    private int maxBytesToConsume(long pos) {
        return isCompressedInput() ? Integer.MAX_VALUE : (int) Math.min(Integer.MAX_VALUE, end - pos);
    }

    private long getFilePosition() throws IOException {
        long retVal;
        if (isCompressedInput() && null != filePosition) {
            retVal = filePosition.getPos();
        } else {
            retVal = pos;
        }
        return retVal;
    }
    
}
