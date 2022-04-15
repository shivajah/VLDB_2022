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
package org.apache.hyracks.dataflow.std.buffermanager;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;

import org.apache.hyracks.api.comm.FrameConstants;
import org.apache.hyracks.api.comm.FrameHelper;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.util.IntSerDeUtils;

public class JoinFrameBufferManager extends FrameBufferManager {
    public JoinFrameBufferManager() {
        super();
        flushedFramesFullness = new ArrayList<>();
    }

    ArrayList<Double> flushedFramesFullness;
    BitSet underfilledFrames;
    int nextFit_PrevFrame = -1;
    int nextFit_PrevSize = -1;

    public void initHY() {
        underfilledFrames = new BitSet();
    }

    public double getFullnessPercentage(int frameIndex) throws HyracksDataException {
        if (buffers.size() > frameIndex) {
            int total = FrameHelper.getTupleCountOffset(buffers.get(frameIndex).capacity());
            return ((double) (total - getFreeSpace(frameIndex)) * 1.0 / (double) total * 1.0);
        } else
            throw new HyracksDataException("Frame does not exist");
    }

    public int getFreeSpace(int frameIndex) throws HyracksDataException {
        if (buffers.size() > frameIndex) {
            ByteBuffer bb = buffers.get(frameIndex);
            byte[] array = bb.array();
            int tupleCount = IntSerDeUtils.getInt(array, FrameHelper.getTupleCountOffset(bb.capacity()));
            int tupleDataEndOffset = tupleCount == 0 ? FrameConstants.TUPLE_START_OFFSET
                    : IntSerDeUtils.getInt(array,
                            FrameHelper.getTupleCountOffset(bb.capacity()) - tupleCount * FrameConstants.SIZE_LEN);
            return FrameHelper.getTupleCountOffset(bb.capacity())
                    - (tupleDataEndOffset + tupleCount * FrameConstants.SIZE_LEN);
        } else
            throw new HyracksDataException("Frame does not exist");
    }
}
