/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hyracks.dataflow.std.join;

import java.io.Serializable;

public class JoinStats implements Serializable {
    private long writes;
    private long reads;

    public long getWrites() {
        return writes;
    }

    public long getReads() {
        return reads;
    }

    public void addToWrites(long write) {
        this.writes += write;
    }

    public void addToReads(long read) {
        this.reads += read;
    }

    @Override
    public String toString() {
        StringBuilder stb = new StringBuilder();
        stb.append("Writes: " + this.writes + " (bytes) " + "Reads: " + (this.reads + " (bytes) "));
        return stb.toString();
    }

    public void addAll(JoinStats other) {
        this.writes += other.getWrites();
        this.reads += other.getReads();
    }
}
