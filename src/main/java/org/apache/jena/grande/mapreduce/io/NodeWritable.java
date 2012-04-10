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
import org.apache.jena.grande.NodeEncoder;
import org.openjena.riot.out.OutputLangUtils;

import com.hp.hpl.jena.graph.Node;

public class NodeWritable extends BinaryComparable implements WritableComparable<BinaryComparable> {

    private Node node;
    private byte[] bytes;
    private int length;
    
    public NodeWritable(){
    	this( Node.ANY );
    }
    
    public NodeWritable(Node node) {
        this.node = node;
        this.bytes = toBytes(node);
        this.length = bytes.length;
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        length = WritableUtils.readVInt(in);
        bytes = new byte[length];
        in.readFully(bytes, 0, length);
        node = NodeEncoder.asNode(new String(bytes));
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
    
    public Node getNode() {
        return node;
    }
    
    private byte[] toBytes(Node node) {
        StringWriter out = new StringWriter();
        out.getBuffer().length();
        OutputLangUtils.output(out, node, null, null);
        try {
            return out.toString().getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new JenaGrandeException(e);
        }
    }

    @Override
    public String toString() {
        return node.toString();
    }

}
