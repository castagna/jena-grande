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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.jena.grande.JenaGrandeException;
import org.openjena.riot.Lang;
import org.openjena.riot.lang.LangNTriples;
import org.openjena.riot.out.OutputLangUtils;
import org.openjena.riot.system.RiotLib;
import org.openjena.riot.tokens.Tokenizer;
import org.openjena.riot.tokens.TokenizerFactory;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;

public class TripleWritable extends BinaryComparable implements WritableComparable<BinaryComparable> {

    private Triple triple;
    private byte[] bytes;
    private int length;
    
    public TripleWritable(){
    	this( new Triple(Node.ANY, Node.ANY, Node.ANY) );
    }
    
    public TripleWritable(Triple triple) {
        this.triple = triple;
        this.bytes = toBytes(triple);
        this.length = bytes.length;
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        length = WritableUtils.readVInt(in);
        bytes = new byte[length];
        in.readFully(bytes, 0, length);
        Tokenizer tokenizer = TokenizerFactory.makeTokenizerASCII(new String(bytes)) ;
        LangNTriples parser = new LangNTriples(tokenizer, RiotLib.profile(Lang.NTRIPLES, null), null) ;
        triple = parser.next();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeVInt(out, length);
        out.write(bytes, 0, length);
    }

    @Override
    public byte[] getBytes() {
        return bytes;
    }

    @Override
    public int getLength() {
        return length;
    }
    
    public Triple getTriple() {
        return triple;
    }
    
    private byte[] toBytes(Triple triple) {
        StringWriter out = new StringWriter();
        out.getBuffer().length();
        OutputLangUtils.output(out, triple, null, null);
        try {
            return out.toString().getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new JenaGrandeException(e);
        }
    }

    @Override
    public String toString() {
        return triple.toString();
    }
}
