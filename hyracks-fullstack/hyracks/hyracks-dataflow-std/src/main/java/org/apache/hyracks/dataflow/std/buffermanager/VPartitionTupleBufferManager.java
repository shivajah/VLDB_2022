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
import java.util.Arrays;
import java.util.BitSet;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hyracks.api.comm.FixedSizeFrame;
import org.apache.hyracks.api.comm.FrameHelper;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksFrameMgrContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FixedSizeFrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.std.structures.TuplePointer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This buffer manager will divide the buffers into given number of partitions.
 * The cleared partition (spilled one in the caller side) can only get no more than one frame.
 */
public class VPartitionTupleBufferManager implements IPartitionedTupleBufferManager {
    private static final Logger LOGGER = LogManager.getLogger();

    public static final IPartitionedMemoryConstrain NO_CONSTRAIN = new IPartitionedMemoryConstrain() {
        @Override
        public int frameLimit(int partitionId) {
            return Integer.MAX_VALUE;
        }
    };

    public enum INSERTION {
        APPEND,
        BESTFIT,
        FIRSTFIT,
        FIRSTFIT_N,
        RANDOM_N,
        RANDOM_N_NO_FF,
        RANDOM_N_CPP_ELEVEN,
        RANDOM_XORSHIFT64,
        MTRANDOM_N,
        RANDOM_BEST_PARAM,
        NEXTFIT,
        HY;

    }

    long totalFramesChecked = 0;

    private IDeallocatableFramePool framePool;
    private JoinFrameBufferManager[] partitionArray;
    // private IFrameBufferManager[] partitionArray;
    private int[] numTuples;
    private final FixedSizeFrame appendFrame;
    private final FixedSizeFrameTupleAppender appender;
    private BufferInfo tempInfo;
    private IPartitionedMemoryConstrain constrain;
    private INSERTION dataInsertion = INSERTION.APPEND;
    private double[] insertionParams = new double[2];
    private BitSet spilledStatus;
    private NextFit nextFitDS;
    private HY hyDS;
    private int[] tuplesInPartition;
    private Random generator;
    // private MersenneTwisterFast MTFRandom;
    private CPPeleven_minstd_rand CPPEleven;
    private long numOfSpilledRecords = 0;
    private long spilledFramesCount = 0;
    private long spilledPhysicalSize = 0;
    // private XorShift64 xorShift64;
    // private Best_Param_LCG_rand best_param_lcg_rand;

    // In case where a frame pool is shared by one or more buffer manager(s), it can be provided from the caller.
    public VPartitionTupleBufferManager(IPartitionedMemoryConstrain constrain, int partitions,
            IDeallocatableFramePool framePool, BitSet spilledStatus) {
        this.constrain = constrain;
        this.framePool = framePool;
        this.partitionArray = new JoinFrameBufferManager[partitions];
        this.numTuples = new int[partitions];
        this.appendFrame = new FixedSizeFrame();
        this.appender = new FixedSizeFrameTupleAppender();
        this.tempInfo = new BufferInfo(null, -1, -1);
        this.spilledStatus = spilledStatus;
        this.tuplesInPartition = new int[partitions];
        this.generator = new Random();
        // this.MTFRandom = new MersenneTwisterFast();
        this.CPPEleven = new CPPeleven_minstd_rand();
        // this.xorShift64 = new XorShift64();
        //this.best_param_lcg_rand = new Best_Param_LCG_rand();

    }

    public VPartitionTupleBufferManager(IHyracksFrameMgrContext ctx, IPartitionedMemoryConstrain constrain,
            int partitions, long frameLimitInBytes) throws HyracksDataException {
        this.constrain = constrain;
        this.framePool = new DeallocatableFramePool(ctx, frameLimitInBytes);
        this.partitionArray = new JoinFrameBufferManager[partitions];
        this.numTuples = new int[partitions];
        this.appendFrame = new FixedSizeFrame();
        this.appender = new FixedSizeFrameTupleAppender();
        this.tempInfo = new BufferInfo(null, -1, -1);
        this.tuplesInPartition = new int[partitions];
        this.generator = new Random();
        //   this.MTFRandom = new MersenneTwisterFast();
        this.CPPEleven = new CPPeleven_minstd_rand();
        //  this.xorShift64 = new XorShift64();
        // this.best_param_lcg_rand = new Best_Param_LCG_rand();
    }

    public int getNumberOfFrames(int pid) {
        return partitionArray[pid].getNumFrames();
    }

    @Override
    public String profileBufferManager() throws HyracksDataException {
        double totalFullness = 0;
        double spilledFullness = 0;
        double inMemFullness = 0;
        //TODO: Flueshed Fullness should be added
        int inMemoryFrames = 0; //frames belonging to in memory partitions
        int spilledFrames = 0; // frames belonging to spilled partitions
        double[] partitionFullness = new double[getNumPartitions()];

        StringBuilder all = new StringBuilder();
        StringBuilder csv = new StringBuilder();
        all.append("\n[{" + "\"Algo\" :\"" + dataInsertion.name() + "\", \n" + "\"Params\":\""
                + Arrays.toString(insertionParams) + "\", \n" + "\"Total_Memory(Frames)\": "
                + framePool.getMemoryBudgetBytes() / framePool.getMinFrameSize() + " ,\n");
        csv.append(dataInsertion.name() + "," + insertionParams[0] + "," + insertionParams[1] + ","
                + framePool.getMemoryBudgetBytes() / framePool.getMinFrameSize() + ",");

        for (int p = 0; p < getNumPartitions(); p++) {
            if (partitionArray[p] == null) {
                all.append(String.format("Partition %s is null:\n", p));
            } else {
                int frames = partitionArray[p].getNumFrames();
                int allFramesofThisPartition =
                        spilledStatus.get(p) ? frames + partitionArray[p].flushedFramesFullness.size() : frames;
                if (spilledStatus.get(p)) {
                    spilledFrames += allFramesofThisPartition;
                    //flushed to disk is already considered in the line above.
                } else {
                    inMemoryFrames += allFramesofThisPartition;
                }
                for (int f = 0; f < frames; f++) {//frames of spilled/inMem parititions that are not flushed
                    partitionArray[p].getFrame(f, tempInfo);
                    if (tempInfo.getBuffer() != appendFrame.getBuffer()) {
                        appendFrame.reset(tempInfo.getBuffer());
                        appender.reset(appendFrame, false);
                    }
                    //double fullnessPercentage = appender.getFrameFullness();
                    double fullnessPercentage = 100 * (1
                            - (partitionArray[p].getFreeSpace(f) * 1.0 / partitionArray[p].buffers.get(f).capacity()));
                    if (spilledStatus.get(p)) {
                        spilledFullness += fullnessPercentage;
                    } else {
                        inMemFullness += fullnessPercentage;
                    }
                    totalFullness += fullnessPercentage;
                    partitionFullness[p] += fullnessPercentage;
                }
                //written to the disk:
                if (spilledStatus.get(p)) {
                    for (int i = 0; i < partitionArray[p].flushedFramesFullness.size(); i++) {
                        spilledFullness += 100 * (partitionArray[p].flushedFramesFullness.get(i));
                    }
                    totalFullness += spilledFullness;
                    partitionFullness[p] += spilledFullness;
                }
                partitionFullness[p] =
                        allFramesofThisPartition > 0 ? partitionFullness[p] / allFramesofThisPartition : -1;
            }
        }
        totalFullness = totalFullness / (inMemoryFrames + spilledFrames);
        double collectiveFullnessForSpilled = (spilledFrames > 0 ? (spilledFullness * 1.0 / spilledFrames) : -1);
        all.append("\"Spilled_Info\":{\n");
        all.append("\"Collective_Fullness(inMemoryFrames)(%)\" : " + collectiveFullnessForSpilled + ",\n");
        all.append("\"Partitions_Fullness(inMemoryFrames)(%)\" : [\n");
        boolean needsComma = false;
        long numOfInMemRecords = 0;
        long numOfSpilledRecords = 0;
        long inMemoryPhysicalSize = 0;
        long spilledPhysicalSize = 0;
        for (int p = 0; p < getNumPartitions(); p++) {
            if (spilledStatus.get(p)) {
                spilledPhysicalSize += getPhysicalSize(p);
                numOfSpilledRecords += getNumTuples(p);
                if (partitionFullness[p] != -1) {
                    if (needsComma) {
                        all.append(",\n");
                    }
                    needsComma = true;
                    all.append("{\"Partition " + p + "\" : " + partitionFullness[p] + "\n }");
                }
            } else {
                inMemoryPhysicalSize += getPhysicalSize(p);
                numOfInMemRecords += getNumTuples(p);
            }
        }
        all.append("\n]\n");
        needsComma = false;
        all.append(",\"FlushedPartitionsFullness\":[\n");
        for (int p = 0; p < getNumPartitions(); p++) {
            if (spilledStatus.get(p) && partitionArray[p].flushedFramesFullness.size() > 0) {
                if (needsComma) {
                    all.append(",\n");
                }
                needsComma = true;
                all.append("{\"Partition " + p + "\" : " + partitionArray[p].flushedFramesFullness.toString() + "\n }");
            }
        }
        all.append("\n]\n");
        all.append("},\n");
        needsComma = false;
        double collectiveFullnessInMemory = inMemFullness / inMemoryFrames;
        all.append("\"InMemory_Info\":{\n");
        all.append("\"Collective_Fullness(%)\" : " + collectiveFullnessInMemory + ",\n");
        all.append("\n \"Partitions_Fullness(%)\" : [\n");
        for (int p = 0; p < getNumPartitions(); p++) {
            if (!spilledStatus.get(p)) {
                if (needsComma) {
                    all.append(",\n");
                }
                needsComma = true;
                all.append("{\"Partition " + p + "\" : " + partitionFullness[p] + "\n }");
            }
        }

        all.append("\n]\n");
        all.append("},\n");
        int totalTuples = 0;
        for (int p = 0; p < getNumPartitions(); p++) {
            all.append("{\"tuples_in_partition" + p + "\" : " + tuplesInPartition[p] + "},");
            totalTuples += tuplesInPartition[p];
        }
        all.append("{\"totalTuples\":" + totalTuples + "},");
        all.append("\"Total_Fullness(%)\": " + totalFullness + ", \n");
        long budgetInFrames = framePool.getMemoryBudgetBytes() / framePool.getMinFrameSize();
        long gap = (budgetInFrames - (inMemoryFrames + spilledStatus.cardinality()));
        all.append("\"Frames_Info\" : [\n { \n \"InMemory_Frames(#) \" : " + inMemoryFrames + ", \n \"Spilled_Frames"
                + "(#) \" : " + spilledFrames + ",\n \"Reserved_Frames_For_Spilled_Partitions(#)\" : "
                + spilledStatus.cardinality() + ",\n \"Total " + "budget(# of frames)\" : "

                + budgetInFrames + ", \n \"Gap(#)\" :" + gap + "}\n]\n");
        all.append(", \"#of_frames_checked\" : " + totalFramesChecked);
        all.append("\n }\n]");
        //LOGGER.debug(all.toString());

        csv.append(collectiveFullnessForSpilled + "," + collectiveFullnessInMemory + "," + spilledFrames + ","
                + inMemoryFrames + "," + spilledStatus.cardinality() + "," + gap + "," + totalFramesChecked + "," + -1
                + spilledPhysicalSize + "," + numOfSpilledRecords + "," + inMemoryPhysicalSize + ","
                + numOfInMemRecords);
        return csv.toString();

    }

    @Override
    public void setDataInsertion(VPartitionTupleBufferManager.INSERTION dataInsertion, double[] insertionParams) {
        this.dataInsertion = dataInsertion;
        this.insertionParams[0] =
                insertionParams[0] > 0 ? insertionParams[0] : (this.dataInsertion == INSERTION.APPEND ? 1 : 0.1);//percentage
        this.insertionParams[1] = insertionParams[1] > 0 ? insertionParams[1] : 0.93;//threshold(for HY)

        if (this.dataInsertion == INSERTION.NEXTFIT) {
            nextFitDS = new NextFit();
        } else if (this.dataInsertion == INSERTION.HY) {
            hyDS = new HY(this.insertionParams[0], this.insertionParams[1]);
        }
    }

    @Override
    public void setConstrain(IPartitionedMemoryConstrain constrain) {
        this.constrain = constrain;
    }

    @Override
    public void reset() throws HyracksDataException {
        for (IFrameBufferManager part : partitionArray) {
            if (part != null) {
                for (int i = 0; i < part.getNumFrames(); i++) {
                    framePool.deAllocateBuffer(part.getFrame(i, tempInfo).getBuffer());
                }
                part.reset();
            }
        }
        Arrays.fill(numTuples, 0);
        appendFrame.reset(null);
    }

    @Override
    public int getNumPartitions() {
        return partitionArray.length;
    }

    @Override
    public int getNumTuples(int partition) {
        return numTuples[partition];
    }

    @Override
    public int getPhysicalSize(int partitionId) {
        int size = 0;
        IFrameBufferManager partition = partitionArray[partitionId];
        if (partition != null) {
            for (int i = 0; i < partition.getNumFrames(); ++i) {
                size += partition.getFrame(i, tempInfo).getLength();
            }
        }
        return size;
    }

    @Override
    public void clearPartition(int partitionId) throws HyracksDataException {
        IFrameBufferManager partition = partitionArray[partitionId];
        if (partition != null) {
            for (int i = 0; i < partition.getNumFrames(); ++i) {
                framePool.deAllocateBuffer(partition.getFrame(i, tempInfo).getBuffer());
            }
            partition.reset();
        }
        numTuples[partitionId] = 0;
    }

    @Override
    public boolean insertTuple(int partition, byte[] byteArray, int[] fieldEndOffsets, int start, int size,
            TuplePointer pointer) throws HyracksDataException {
        int actualSize = calculateActualSize(fieldEndOffsets, size);
        int fid = -1;
        if (actualSize > framePool.getMinFrameSize()) {
            fid = createNewBuffer(partition, actualSize);
        } else {
            switch (dataInsertion) {
                case APPEND:
                    fid = getLastNBuffersOrCreateNewIfNotExist(partition, actualSize, (int) this.insertionParams[0]);
                    break;
                case BESTFIT:
                    fid = getBestFitBufferOrCreateNewIfNotExist(partition, actualSize);
                    break;
                case FIRSTFIT_N:
                    int numberOfFramesToCheck = partitionArray[partition] == null ? -1
                            : (int) Math.ceil(this.insertionParams[0] * partitionArray[partition].getNumFrames());
                    fid = getFirstFitBufferOrCreateNewIfNotExist(partition, actualSize, numberOfFramesToCheck);
                    break;
                case FIRSTFIT:
                    fid = getFirstFitBufferOrCreateNewIfNotExist(partition, actualSize, -1);
                    break;
                case RANDOM_N:
                    fid = getRandomFitBufferOrCreateNewIfNotExist(partition, actualSize, this.insertionParams[0]);
                    break;
                case RANDOM_N_CPP_ELEVEN:
                    fid = getRandomFitBufferCPPElevenOrCreateNewIfNotExist(partition, actualSize,
                            this.insertionParams[0]);
                    break;
                case RANDOM_N_NO_FF:
                    fid = getRandomFitBufferNOFFOrCreateNewIfNotExist(partition, actualSize, this.insertionParams[0]);
                    break;
                case NEXTFIT:
                    fid = nextFitDS.getNextFitBufferOrCreateNewIfNotExist(partition, actualSize);
                    break;
                case HY:
                    fid = hyDS.getHYBufferOrCreateNewIfNotExist(partition, actualSize);
                    break;
            }
        }
        if (fid < 0) {
            return false;
        }
        partitionArray[partition].getFrame(fid, tempInfo);
        int tid = appendTupleToBuffer(tempInfo, fieldEndOffsets, byteArray, start, size);
        if (tid < 0) {
            fid = createNewBuffer(partition, actualSize);
            if (fid < 0) {
                return false;
            }
            partitionArray[partition].getFrame(fid, tempInfo);
            tid = appendTupleToBuffer(tempInfo, fieldEndOffsets, byteArray, start, size);
        }
        pointer.reset(makeGroupFrameId(partition, fid), tid);
        numTuples[partition]++;
        if (dataInsertion == INSERTION.HY) {
            hyDS.updateFrameStat(partition, fid);
        }
        tuplesInPartition[partition]++;
        return true;
    }

    @Override
    public boolean insertTuple(int partition, IFrameTupleAccessor tupleAccessor, int tupleId, TuplePointer pointer)
            throws HyracksDataException {
        return insertTuple(partition, tupleAccessor.getBuffer().array(), null,
                tupleAccessor.getTupleStartOffset(tupleId), tupleAccessor.getTupleLength(tupleId), pointer);
    }

    @Override
    public void cancelInsertTuple(int partition) throws HyracksDataException {
        int fid = getLastBuffer(partition);
        if (fid < 0) {
            throw new HyracksDataException("Couldn't get the last frame for the given partition.");
        }
        partitionArray[partition].getFrame(fid, tempInfo);
        deleteTupleFromBuffer(tempInfo);
        numTuples[partition]--;
    }

    public static int calculateActualSize(int[] fieldEndOffsets, int size) {
        if (fieldEndOffsets != null) {
            return FrameHelper.calcRequiredSpace(fieldEndOffsets.length, size);
        }
        return FrameHelper.calcRequiredSpace(0, size);
    }

    private int makeGroupFrameId(int partition, int fid) {
        return fid * getNumPartitions() + partition;
    }

    private int parsePartitionId(int externalFrameId) {
        return externalFrameId % getNumPartitions();
    }

    private int parseFrameIdInPartition(int externalFrameId) {
        return externalFrameId / getNumPartitions();
    }

    private int createNewBuffer(int partition, int size) throws HyracksDataException {
        ByteBuffer newBuffer = requestNewBufferFromPool(size, partition);
        if (newBuffer == null) {
            return -1;
        }
        appendFrame.reset(newBuffer);
        appender.reset(appendFrame, true);
        return partitionArray[partition].insertFrame(newBuffer);
    }

    private ByteBuffer requestNewBufferFromPool(int recordSize, int partition) throws HyracksDataException {
        int frameSize = FrameHelper.calcAlignedFrameSizeToStore(0, recordSize, framePool.getMinFrameSize());
        if ((double) frameSize / (double) framePool.getMinFrameSize() + getPhysicalSize(partition) > constrain
                .frameLimit(partition)) {
            return null;
        }
        return framePool.allocateFrame(frameSize);
    }

    private int appendTupleToBuffer(BufferInfo bufferInfo, int[] fieldEndOffsets, byte[] byteArray, int start, int size)
            throws HyracksDataException {
        assert bufferInfo.getStartOffset() == 0 : "Haven't supported yet in FrameTupleAppender";
        if (bufferInfo.getBuffer() != appendFrame.getBuffer()) {
            appendFrame.reset(bufferInfo.getBuffer());
            appender.reset(appendFrame, false);
        }
        if (fieldEndOffsets == null) {
            if (appender.append(byteArray, start, size)) {
                return appender.getTupleCount() - 1;
            }
        } else {
            if (appender.append(fieldEndOffsets, byteArray, start, size)) {
                return appender.getTupleCount() - 1;
            }
        }

        return -1;
    }

    private void deleteTupleFromBuffer(BufferInfo bufferInfo) throws HyracksDataException {
        if (bufferInfo.getBuffer() != appendFrame.getBuffer()) {
            appendFrame.reset(bufferInfo.getBuffer());
            appender.reset(appendFrame, false);
        }
        if (!appender.cancelAppend()) {
            throw new HyracksDataException("Undoing the last insertion in the given frame couldn't be done.");
        }
    }

    private int getLastNBuffersOrCreateNewIfNotExist(int partition, int actualSize, int checkNFrames)
            throws HyracksDataException {
        if (partitionArray[partition] == null || partitionArray[partition].getNumFrames() == 0) {
            partitionArray[partition] = new JoinFrameBufferManager();
            return createNewBuffer(partition, actualSize);
        }

        int limit = checkNFrames <= 0 ? 0 : Math.max(0, partitionArray[partition].getNumFrames() - checkNFrames);
        for (int index = partitionArray[partition].getNumFrames() - 1; index >= limit; index--) {
            totalFramesChecked++;
            if (partitionArray[partition].getFreeSpace(index) >= actualSize) {
                return index;
            }
        }
        return partitionArray[partition].getNumFrames() - 1;
    }

    private int getFirstFitBufferOrCreateNewIfNotExist(int partition, int actualSize, int checkNFrames)
            throws HyracksDataException {

        if (partitionArray[partition] == null) {
            partitionArray[partition] = new JoinFrameBufferManager();
            return createNewBuffer(partition, actualSize);
        }

        int limit = checkNFrames <= 0 ? 0 : Math.max(0, partitionArray[partition].getNumFrames() - checkNFrames);

        for (int index = partitionArray[partition].getNumFrames() - 1; index >= limit; index--) {
            totalFramesChecked++;
            if (partitionArray[partition].getFreeSpace(index) >= actualSize) {
                return index;
            }
        }

        return createNewBuffer(partition, actualSize);
    }

    private int getBestFitBufferOrCreateNewIfNotExist(int partition, int actualSize) throws HyracksDataException {

        int minSpaceIndex = -1;
        int minSpaceAmount = Integer.MAX_VALUE;
        if (partitionArray[partition] == null) {
            partitionArray[partition] = new JoinFrameBufferManager();
        }
        JoinFrameBufferManager p = partitionArray[partition];
        int numFrames = p.getNumFrames();
        for (int index = 0; index < numFrames; index++) {
            totalFramesChecked++;
            int remainingSpace = p.getFreeSpace(index) - actualSize;
            if (remainingSpace >= 0 && remainingSpace < minSpaceAmount) {
                minSpaceAmount = remainingSpace;
                minSpaceIndex = index;
            }
        }

        if (minSpaceIndex >= 0) {
            return minSpaceIndex;
        }
        return createNewBuffer(partition, actualSize);
    }

    //Check percentage*numberOfFrames randomly, and if could not insert, append a page
    private int getRandomFitBufferOrCreateNewIfNotExist(int partition, int actualSize, double percentage)
            throws HyracksDataException {
        //TODO: make the 0.1 percentage to be a variable coming from the query
        Random generator = new Random();
        if (partitionArray[partition] == null) {
            partitionArray[partition] = new JoinFrameBufferManager();
        }
        JoinFrameBufferManager p = partitionArray[partition];
        int numFrames = p.getNumFrames();
        int numberOfFramesToCheck = (int) Math.floor(percentage * numFrames);

        for (int i = 0; i < numberOfFramesToCheck; i++) {
            int nextChosenFrame = generator.nextInt(numFrames);
            totalFramesChecked++;
            int remainingSpace = p.getFreeSpace(nextChosenFrame) - actualSize;
            if (remainingSpace >= 0) {
                return nextChosenFrame;
            }
        }
        return createNewBuffer(partition, actualSize);
    }

    //    //Check percentage*numberOfFrames randomly, and if could not insert, append a page
    //    private int getRandomFitBufferBestParamOrCreateNewIfNotExist(int partition, int actualSize, double percentage)
    //            throws HyracksDataException {
    //        if (partitionArray[partition] == null) {
    //            partitionArray[partition] = new JoinFrameBufferManager();
    //        }
    //        JoinFrameBufferManager p = partitionArray[partition];
    //        int numFrames = p.getNumFrames();
    //        int numberOfFramesToCheck = (int) Math.floor(percentage * numFrames);
    //        if (numberOfFramesToCheck == 0) {// the number of frames is too few, run FF
    //            return getFirstFitBufferOrCreateNewIfNotExist(partition, actualSize, -1);
    //        } else {
    //            for (int i = 0; i < numberOfFramesToCheck; i++) {
    //                int nextChosenFrame = best_param_lcg_rand.nextInt(numFrames);
    //                totalFramesChecked++;
    //                int remainingSpace = p.getFreeSpace(nextChosenFrame, true) - actualSize;
    //                if (remainingSpace >= 0) {
    //                    return nextChosenFrame;
    //                }
    //            }
    //        }
    //        return createNewBuffer(partition, actualSize);
    //    }

    //Check percentage*numberOfFrames randomly, and if could not insert, append a page
    private int getRandomFitBufferNOFFOrCreateNewIfNotExist(int partition, int actualSize, double percentage)
            throws HyracksDataException {
        if (partitionArray[partition] == null) {
            partitionArray[partition] = new JoinFrameBufferManager();
        }
        JoinFrameBufferManager p = partitionArray[partition];
        int numFrames = p.getNumFrames();
        int numberOfFramesToCheck = (int) Math.ceil(percentage * numFrames);

        for (int i = 0; i < numberOfFramesToCheck; i++) {
            int nextChosenFrame = generator.nextInt(numFrames);
            totalFramesChecked++;
            int remainingSpace = p.getFreeSpace(nextChosenFrame) - actualSize;
            if (remainingSpace >= 0) {
                return nextChosenFrame;
            }
        }

        return createNewBuffer(partition, actualSize);
    }

    //    private int getRandomFitBufferMersenneTwisterFastOrCreateNewIfNotExist(int partition, int actualSize,
    //            double percentage) throws HyracksDataException {
    //        if (partitionArray[partition] == null) {
    //            partitionArray[partition] = new JoinFrameBufferManager();
    //        }
    //        JoinFrameBufferManager p = partitionArray[partition];
    //        int numFrames = p.getNumFrames();
    //        int numberOfFramesToCheck = (int) Math.floor(percentage * numFrames);
    //        if (numberOfFramesToCheck == 0) {// the number of frames is too few, run FF
    //            return getFirstFitBufferOrCreateNewIfNotExist(partition, actualSize, -1);
    //        } else {
    //            for (int i = 0; i < numberOfFramesToCheck; i++) {
    //                int nextChosenFrame = MTFRandom.nextInt(numFrames);
    //                totalFramesChecked++;
    //                int remainingSpace = p.getFreeSpace(nextChosenFrame, true) - actualSize;
    //                if (remainingSpace >= 0) {
    //                    return nextChosenFrame;
    //                }
    //            }
    //        }
    //        return createNewBuffer(partition, actualSize);
    //    }

    private int getRandomFitBufferCPPElevenOrCreateNewIfNotExist(int partition, int actualSize, double percentage)
            throws HyracksDataException {

        if (partitionArray[partition] == null) {
            partitionArray[partition] = new JoinFrameBufferManager();
            return createNewBuffer(partition, actualSize);
        }
        JoinFrameBufferManager p = partitionArray[partition];
        int numFrames = p.getNumFrames();
        int numberOfFramesToCheck = (int) Math.ceil(percentage * numFrames);

        for (int i = 0; i < numberOfFramesToCheck; i++) {
            int nextChosenFrame = CPPEleven.nextInt(numFrames);
            totalFramesChecked++;
            int remainingSpace = p.getFreeSpace(nextChosenFrame) - actualSize;
            if (remainingSpace >= 0) {
                return nextChosenFrame;
            }
        }

        return createNewBuffer(partition, actualSize);
    }

    //    private int getRandomFitBufferXORSHIFT64OrCreateNewIfNotExist(int partition, int actualSize, double percentage)
    //            throws HyracksDataException {
    //        if (partitionArray[partition] == null) {
    //            partitionArray[partition] = new JoinFrameBufferManager();
    //        }
    //        JoinFrameBufferManager p = partitionArray[partition];
    //        int numFrames = p.getNumFrames();
    //        int numberOfFramesToCheck = (int) Math.floor(percentage * numFrames);
    //        if (numberOfFramesToCheck == 0) {// the number of frames is too few, run FF
    //            return getFirstFitBufferOrCreateNewIfNotExist(partition, actualSize, -1);
    //        } else {
    //            for (int i = 0; i < numberOfFramesToCheck; i++) {
    //                int nextChosenFrame = xorShift64.nextInt(numFrames);
    //                totalFramesChecked++;
    //                int remainingSpace = p.getFreeSpace(nextChosenFrame, true) - actualSize;
    //                if (remainingSpace >= 0) {
    //                    return nextChosenFrame;
    //                }
    //            }
    //        }
    //        return createNewBuffer(partition, actualSize);
    //    }

    private int getLastBuffer(int partition) {
        return partitionArray[partition].getNumFrames() - 1;
    }

    @Override
    public void close() {
        for (IFrameBufferManager part : partitionArray) {
            if (part != null) {
                part.close();
            }
        }
        framePool.close();
        Arrays.fill(partitionArray, null);
    }

    @Override
    public ITuplePointerAccessor getTuplePointerAccessor(final RecordDescriptor recordDescriptor) {
        return new AbstractTuplePointerAccessor() {
            FrameTupleAccessor innerAccessor = new FrameTupleAccessor(recordDescriptor);

            @Override
            IFrameTupleAccessor getInnerAccessor() {
                return innerAccessor;
            }

            @Override
            void resetInnerAccessor(TuplePointer tuplePointer) {
                partitionArray[parsePartitionId(tuplePointer.getFrameIndex())]
                        .getFrame(parseFrameIdInPartition(tuplePointer.getFrameIndex()), tempInfo);
                innerAccessor.reset(tempInfo.getBuffer(), tempInfo.getStartOffset(), tempInfo.getLength());
            }
        };
    }

    @Override
    public void flushPartition(int pid, IFrameWriter writer, boolean flushToDisk) throws HyracksDataException {
        JoinFrameBufferManager partition = partitionArray[pid];
        if (partition != null && getNumTuples(pid) > 0) {
            for (int i = 0; i < partition.getNumFrames(); ++i) {
                partition.getFrame(i, tempInfo);
                if (flushToDisk) {
                    partition.flushedFramesFullness.add(partition.getFullnessPercentage(i));
                    numOfSpilledRecords += getNumTuples(pid);
                    spilledFramesCount += getNumberOfFrames(pid);
                    spilledPhysicalSize += getPhysicalSize(pid);
                }
                partition.flushedFramesFullness.add(partition.getFullnessPercentage(i));
                tempInfo.getBuffer().position(tempInfo.getStartOffset());
                tempInfo.getBuffer().limit(tempInfo.getStartOffset() + tempInfo.getLength());
                writer.nextFrame(tempInfo.getBuffer());
            }
        }

        if (dataInsertion == INSERTION.NEXTFIT) {
            partitionArray[pid].nextFit_PrevSize = -1;
            partitionArray[pid].nextFit_PrevFrame = -1;
        }
    }

    private class NextFit {

        public int getNextFitBufferOrCreateNewIfNotExist(int partition, int currentRecordSize)
                throws HyracksDataException {
            if (partitionArray[partition] == null) {
                partitionArray[partition] = new JoinFrameBufferManager();
                int fid = createNewBuffer(partition, currentRecordSize);
                partitionArray[partition].nextFit_PrevFrame = fid;
                partitionArray[partition].nextFit_PrevSize = currentRecordSize;
                return fid;
            }
            if (partitionArray[partition].nextFit_PrevFrame >= 0) {
                int endingFrame = currentRecordSize >= partitionArray[partition].nextFit_PrevSize
                        ? partitionArray[partition].getNumFrames() - 1 : 0;

                int nextFrameId = searchForward(partitionArray[partition].nextFit_PrevFrame, endingFrame, partition,
                        currentRecordSize);
                if (nextFrameId >= 0) {
                    partitionArray[partition].nextFit_PrevFrame = nextFrameId;
                    partitionArray[partition].nextFit_PrevSize = currentRecordSize;
                    return nextFrameId;
                }
            }
            int fid = createNewBuffer(partition, currentRecordSize);
            partitionArray[partition].nextFit_PrevFrame = fid;
            partitionArray[partition].nextFit_PrevSize = currentRecordSize;
            return fid;
        }

        private int searchForward(int startingFrame, int endingFrame, int partition, int recordSize)
                throws HyracksDataException {
            if (endingFrame >= startingFrame) {//search from prev toward end
                for (int i = startingFrame; i <= endingFrame; i++) {
                    totalFramesChecked++;
                    int remainingSpace = partitionArray[partition].getFreeSpace(i) - recordSize;
                    if (remainingSpace >= 0) {
                        return i;
                    }
                }
            } else {//starts from prev toward frame0
                for (int i = startingFrame; i >= endingFrame; i--) {
                    totalFramesChecked++;
                    int remainingSpace = partitionArray[partition].getFreeSpace(i) - recordSize;
                    if (remainingSpace >= 0) {
                        return i;
                    }
                } // if not found then from prev toward last frame
                for (int i = startingFrame; i <= partitionArray[partition].getNumFrames() - 1; i++) {
                    totalFramesChecked++;
                    int remainingSpace = partitionArray[partition].getFreeSpace(i) - recordSize;
                    if (remainingSpace >= 0) {
                        return i;
                    }
                }
            }
            return -1;
        }

    }

    @Override
    public IPartitionedMemoryConstrain getConstrain() {
        return constrain;
    }

    /* A mix of FF(n) (in the paper AO(n)) and having threshold of fullness. If all frames' fullness is above the
    threshold then FF(n) will happen, otherwise, NEXTFIT.*/
    private class HY {

        private double threshold;
        private double percentage;
        private int nextFitPrevIndex = 0;
        private int nextFitPrevRecSize = 0;

        public HY(double percentage, double threshold) {
            this.threshold = threshold;
            this.percentage = percentage;
            for (int p = 0; p < partitionArray.length; p++) {
                partitionArray[p] = new JoinFrameBufferManager();
                partitionArray[p].initHY();
            }
        }

        public void updateFrameStat(int partition, int frameId) throws HyracksDataException {//Call this after the
            // record is inserted.
            double fullness = partitionArray[partition].getFullnessPercentage(frameId);//Fullness after the record is
            // inserted
            boolean wasUnderfilled = partitionArray[partition].underfilledFrames.get(frameId);
            if (fullness > threshold && wasUnderfilled) {
                partitionArray[partition].underfilledFrames.set(frameId, false);
            } else {
                if (!wasUnderfilled) {
                    partitionArray[partition].underfilledFrames.set(frameId, true);
                }
            }
        }

        public int getHYBufferOrCreateNewIfNotExist(int partition, int currentRecordSize) throws HyracksDataException {
            if (partitionArray[partition] == null) {
                partitionArray[partition] = new JoinFrameBufferManager();
            }
            if (partitionArray[partition].underfilledFrames.size() == 0) {//FF(n)
                int checkNFrames = (int) Math.floor(this.percentage * partitionArray[partition].getNumFrames());
                return getFirstFitBufferOrCreateNewIfNotExist(partition, currentRecordSize, checkNFrames);
            } else {//NF on underFilled
                int startingIndex = currentRecordSize >= nextFitPrevRecSize ? nextFitPrevIndex : 0;//index on underfilled
                int nextToCheck = partitionArray[partition].underfilledFrames.nextSetBit(startingIndex);
                while (nextToCheck >= 0) {
                    totalFramesChecked++;
                    if (partitionArray[partition].getFreeSpace(nextToCheck) >= currentRecordSize) {
                        return nextToCheck;
                    }
                    nextToCheck = partitionArray[partition].underfilledFrames.nextSetBit(nextToCheck + 1);
                }
                return createNewBuffer(partition, currentRecordSize);
            }
        }
    }
    //
    //    /**
    //     * <h3>MersenneTwister and MersenneTwisterFast</h3>
    //     * <p><b>Version 22</b>, based on version MT199937(99/10/29)
    //     * of the Mersenne Twister algorithm found at
    //     * <a href="http://www.math.keio.ac.jp/matumoto/emt.html">
    //     * The Mersenne Twister Home Page</a>, with the initialization
    //     * improved using the new 2002/1/26 initialization algorithm
    //     * By Sean Luke, October 2004.
    //     *
    //     * <p><b>MersenneTwister</b> is a drop-in subclass replacement
    //     * for java.util.Random.  It is properly synchronized and
    //     * can be used in a multithreaded environment.  On modern VMs such
    //     * as HotSpot, it is approximately 1/3 slower than java.util.Random.
    //     *
    //     * <p><b>MersenneTwisterFast</b> is not a subclass of java.util.Random.  It has
    //     * the same public methods as Random does, however, and it is
    //     * algorithmically identical to MersenneTwister.  MersenneTwisterFast
    //     * has hard-code inlined all of its methods directly, and made all of them
    //     * final (well, the ones of consequence anyway).  Further, these
    //     * methods are <i>not</i> synchronized, so the same MersenneTwisterFast
    //     * instance cannot be shared by multiple threads.  But all this helps
    //     * MersenneTwisterFast achieve well over twice the speed of MersenneTwister.
    //     * java.util.Random is about 1/3 slower than MersenneTwisterFast.
    //     *
    //     * <h3>About the Mersenne Twister</h3>
    //     * <p>This is a Java version of the C-program for MT19937: Integer version.
    //     * The MT19937 algorithm was created by Makoto Matsumoto and Takuji Nishimura,
    //     * who ask: "When you use this, send an email to: matumoto@math.keio.ac.jp
    //     * with an appropriate reference to your work".  Indicate that this
    //     * is a translation of their algorithm into Java.
    //     *
    //     * <p><b>Reference. </b>
    //     * Makato Matsumoto and Takuji Nishimura,
    //     * "Mersenne Twister: A 623-Dimensionally Equidistributed Uniform
    //     * Pseudo-Random Number Generator",
    //     * <i>ACM Transactions on Modeling and. Computer Simulation,</i>
    //     * Vol. 8, No. 1, January 1998, pp 3--30.
    //     *
    //     * <h3>About this Version</h3>
    //     *
    //     * <p><b>Changes since V21:</b> Minor documentation HTML fixes.
    //     *
    //     * <p><b>Changes since V20:</b> Added clearGuassian().  Modified stateEquals()
    //     * to be synchronizd on both objects for MersenneTwister, and changed its
    //     * documentation.  Added synchronization to both setSeed() methods, to
    //     * writeState(), and to readState() in MersenneTwister.  Removed synchronization
    //     * from readObject() in MersenneTwister.
    //     *
    //     * <p><b>Changes since V19:</b> nextFloat(boolean, boolean) now returns float,
    //     * not double.
    //     *
    //     * <p><b>Changes since V18:</b> Removed old final declarations, which used to
    //     * potentially speed up the code, but no longer.
    //     *
    //     * <p><b>Changes since V17:</b> Removed vestigial references to &amp;= 0xffffffff
    //     * which stemmed from the original C code.  The C code could not guarantee that
    //     * ints were 32 bit, hence the masks.  The vestigial references in the Java
    //     * code were likely optimized out anyway.
    //     *
    //     * <p><b>Changes since V16:</b> Added nextDouble(includeZero, includeOne) and
    //     * nextFloat(includeZero, includeOne) to allow for half-open, fully-closed, and
    //     * fully-open intervals.
    //     *
    //     * <p><b>Changes Since V15:</b> Added serialVersionUID to quiet compiler warnings
    //     * from Sun's overly verbose compilers as of JDK 1.5.
    //     *
    //     * <p><b>Changes Since V14:</b> made strictfp, with StrictMath.log and StrictMath.sqrt
    //     * in nextGaussian instead of Math.log and Math.sqrt.  This is largely just to be safe,
    //     * as it presently makes no difference in the speed, correctness, or results of the
    //     * algorithm.
    //     *
    //     * <p><b>Changes Since V13:</b> clone() method CloneNotSupportedException removed.
    //     *
    //     * <p><b>Changes Since V12:</b> clone() method added.
    //     *
    //     * <p><b>Changes Since V11:</b> stateEquals(...) method added.  MersenneTwisterFast
    //     * is equal to other MersenneTwisterFasts with identical state; likewise
    //     * MersenneTwister is equal to other MersenneTwister with identical state.
    //     * This isn't equals(...) because that requires a contract of immutability
    //     * to compare by value.
    //     *
    //     * <p><b>Changes Since V10:</b> A documentation error suggested that
    //     * setSeed(int[]) required an int[] array 624 long.  In fact, the array
    //     * can be any non-zero length.  The new version also checks for this fact.
    //     *
    //     * <p><b>Changes Since V9:</b> readState(stream) and writeState(stream)
    //     * provided.
    //     *
    //     * <p><b>Changes Since V8:</b> setSeed(int) was only using the first 28 bits
    //     * of the seed; it should have been 32 bits.  For small-number seeds the
    //     * behavior is identical.
    //     *
    //     * <p><b>Changes Since V7:</b> A documentation error in MersenneTwisterFast
    //     * (but not MersenneTwister) stated that nextDouble selects uniformly from
    //     * the full-open interval [0,1].  It does not.  nextDouble's contract is
    //     * identical across MersenneTwisterFast, MersenneTwister, and java.util.Random,
    //     * namely, selection in the half-open interval [0,1).  That is, 1.0 should
    //     * not be returned.  A similar contract exists in nextFloat.
    //     *
    //     * <p><b>Changes Since V6:</b> License has changed from LGPL to BSD.
    //     * New timing information to compare against
    //     * java.util.Random.  Recent versions of HotSpot have helped Random increase
    //     * in speed to the point where it is faster than MersenneTwister but slower
    //     * than MersenneTwisterFast (which should be the case, as it's a less complex
    //     * algorithm but is synchronized).
    //     *
    //     * <p><b>Changes Since V5:</b> New empty constructor made to work the same
    //     * as java.util.Random -- namely, it seeds based on the current time in
    //     * milliseconds.
    //     *
    //     * <p><b>Changes Since V4:</b> New initialization algorithms.  See
    //     * (see <a href="http://www.math.keio.ac.jp/matumoto/MT2002/emt19937ar.html">
    //     * http://www.math.keio.ac.jp/matumoto/MT2002/emt19937ar.html</a>)
    //     *
    //     * <p>The MersenneTwister code is based on standard MT19937 C/C++
    //     * code by Takuji Nishimura,
    //     * with suggestions from Topher Cooper and Marc Rieffel, July 1997.
    //     * The code was originally translated into Java by Michael Lecuyer,
    //     * January 1999, and the original code is Copyright (c) 1999 by Michael Lecuyer.
    //     *
    //     * <h3>Java notes</h3>
    //     *
    //     * <p>This implementation implements the bug fixes made
    //     * in Java 1.2's version of Random, which means it can be used with
    //     * earlier versions of Java.  See
    //     * <a href="http://www.javasoft.com/products/jdk/1.2/docs/api/java/util/Random.html">
    //     * the JDK 1.2 java.util.Random documentation</a> for further documentation
    //     * on the random-number generation contracts made.  Additionally, there's
    //     * an undocumented bug in the JDK java.util.Random.nextBytes() method,
    //     * which this code fixes.
    //     *
    //     * <p> Just like java.util.Random, this
    //     * generator accepts a long seed but doesn't use all of it.  java.util.Random
    //     * uses 48 bits.  The Mersenne Twister instead uses 32 bits (int size).
    //     * So it's best if your seed does not exceed the int range.
    //     *
    //     * <p>MersenneTwister can be used reliably
    //     * on JDK version 1.1.5 or above.  Earlier Java versions have serious bugs in
    //     * java.util.Random; only MersenneTwisterFast (and not MersenneTwister nor
    //     * java.util.Random) should be used with them.
    //     *
    //     * <h3>License</h3>
    //     *
    //     * Copyright (c) 2003 by Sean Luke. <br>
    //     * Portions copyright (c) 1993 by Michael Lecuyer. <br>
    //     * All rights reserved. <br>
    //     *
    //     * <p>Redistribution and use in source and binary forms, with or without
    //     * modification, are permitted provided that the following conditions are met:
    //     * <ul>
    //     * <li> Redistributions of source code must retain the above copyright notice,
    //     * this list of conditions and the following disclaimer.
    //     * <li> Redistributions in binary form must reproduce the above copyright notice,
    //     * this list of conditions and the following disclaimer in the documentation
    //     * and/or other materials provided with the distribution.
    //     * <li> Neither the name of the copyright owners, their employers, nor the
    //     * names of its contributors may be used to endorse or promote products
    //     * derived from this software without specific prior written permission.
    //     * </ul>
    //     * <p>THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
    //     * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
    //     * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
    //     * DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT OWNERS OR CONTRIBUTORS BE
    //     * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
    //     * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
    //     * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
    //     * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
    //     * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
    //     * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
    //     * POSSIBILITY OF SUCH DAMAGE.
    //     *
    //     @version 22
    //     */
    //
    //    // Note: this class is hard-inlined in all of its methods.  This makes some of
    //    // the methods well-nigh unreadable in their complexity.  In fact, the Mersenne
    //    // Twister is fairly easy code to understand: if you're trying to get a handle
    //    // on the code, I strongly suggest looking at MersenneTwister.java first.
    //    // -- Sean
    //
    //    private static strictfp class MersenneTwisterFast implements Serializable, Cloneable {
    //        // Serialization
    //        private static final long serialVersionUID = -8219700664442619525L; // locked as of Version 15
    //
    //        // Period parameters
    //        private static final int N = 624;
    //        private static final int M = 397;
    //        private static final int MATRIX_A = 0x9908b0df; //    private static final * constant vector a
    //        private static final int UPPER_MASK = 0x80000000; // most significant w-r bits
    //        private static final int LOWER_MASK = 0x7fffffff; // least significant r bits
    //
    //        // Tempering parameters
    //        private static final int TEMPERING_MASK_B = 0x9d2c5680;
    //        private static final int TEMPERING_MASK_C = 0xefc60000;
    //
    //        private int mt[]; // the array for the state vector
    //        private int mti; // mti==N+1 means mt[N] is not initialized
    //        private int mag01[];
    //
    //        // a good initial seed (of int size, though stored in a long)
    //        //private static final long GOOD_SEED = 4357;
    //
    //        private double __nextNextGaussian;
    //        private boolean __haveNextNextGaussian;
    //
    //        /* We're overriding all internal data, to my knowledge, so this should be okay */
    //        public Object clone() {
    //            try {
    //                MersenneTwisterFast f = (MersenneTwisterFast) (super.clone());
    //                f.mt = (int[]) (mt.clone());
    //                f.mag01 = (int[]) (mag01.clone());
    //                return f;
    //            } catch (CloneNotSupportedException e) {
    //                throw new InternalError();
    //            } // should never happen
    //        }
    //
    //        /** Returns true if the MersenneTwisterFast's current internal state is equal to another MersenneTwisterFast.
    //         This is roughly the same as equals(other), except that it compares based on value but does not
    //         guarantee the contract of immutability (obviously random number generators are immutable).
    //         Note that this does NOT check to see if the internal gaussian storage is the same
    //         for both.  You can guarantee that the internal gaussian storage is the same (and so the
    //         nextGaussian() methods will return the same values) by calling clearGaussian() on both
    //         objects. */
    //        public boolean stateEquals(MersenneTwisterFast other) {
    //            if (other == this)
    //                return true;
    //            if (other == null)
    //                return false;
    //
    //            if (mti != other.mti)
    //                return false;
    //            for (int x = 0; x < mag01.length; x++)
    //                if (mag01[x] != other.mag01[x])
    //                    return false;
    //            for (int x = 0; x < mt.length; x++)
    //                if (mt[x] != other.mt[x])
    //                    return false;
    //            return true;
    //        }
    //
    //        /** Reads the entire state of the MersenneTwister RNG from the stream */
    //        public void readState(DataInputStream stream) throws IOException {
    //            int len = mt.length;
    //            for (int x = 0; x < len; x++)
    //                mt[x] = stream.readInt();
    //
    //            len = mag01.length;
    //            for (int x = 0; x < len; x++)
    //                mag01[x] = stream.readInt();
    //
    //            mti = stream.readInt();
    //            __nextNextGaussian = stream.readDouble();
    //            __haveNextNextGaussian = stream.readBoolean();
    //        }
    //
    //        /** Writes the entire state of the MersenneTwister RNG to the stream */
    //        public void writeState(DataOutputStream stream) throws IOException {
    //            int len = mt.length;
    //            for (int x = 0; x < len; x++)
    //                stream.writeInt(mt[x]);
    //
    //            len = mag01.length;
    //            for (int x = 0; x < len; x++)
    //                stream.writeInt(mag01[x]);
    //
    //            stream.writeInt(mti);
    //            stream.writeDouble(__nextNextGaussian);
    //            stream.writeBoolean(__haveNextNextGaussian);
    //        }
    //
    //        /**
    //         * Constructor using the default seed.
    //         */
    //        public MersenneTwisterFast() {
    //            this(System.currentTimeMillis());
    //        }
    //
    //        /**
    //         * Constructor using a given seed.  Though you pass this seed in
    //         * as a long, it's best to make sure it's actually an integer.
    //         *
    //         */
    //        public MersenneTwisterFast(long seed) {
    //            setSeed(seed);
    //        }
    //
    //        /**
    //         * Constructor using an array of integers as seed.
    //         * Your array must have a non-zero length.  Only the first 624 integers
    //         * in the array are used; if the array is shorter than this then
    //         * integers are repeatedly used in a wrap-around fashion.
    //         */
    //        public MersenneTwisterFast(int[] array) {
    //            setSeed(array);
    //        }
    //
    //        /**
    //         * Initalize the pseudo random number generator.  Don't
    //         * pass in a long that's bigger than an int (Mersenne Twister
    //         * only uses the first 32 bits for its seed).
    //         */
    //
    //        public void setSeed(long seed) {
    //            // Due to a bug in java.util.Random clear up to 1.2, we're
    //            // doing our own Gaussian variable.
    //            __haveNextNextGaussian = false;
    //
    //            mt = new int[N];
    //
    //            mag01 = new int[2];
    //            mag01[0] = 0x0;
    //            mag01[1] = MATRIX_A;
    //
    //            mt[0] = (int) (seed & 0xffffffff);
    //            for (mti = 1; mti < N; mti++) {
    //                mt[mti] = (1812433253 * (mt[mti - 1] ^ (mt[mti - 1] >>> 30)) + mti);
    //                /* See Knuth TAOCP Vol2. 3rd Ed. P.106 for multiplier. */
    //                /* In the previous versions, MSBs of the seed affect   */
    //                /* only MSBs of the array mt[].                        */
    //                /* 2002/01/09 modified by Makoto Matsumoto             */
    //                // mt[mti] &= 0xffffffff;
    //                /* for >32 bit machines */
    //            }
    //        }
    //
    //        /**
    //         * Sets the seed of the MersenneTwister using an array of integers.
    //         * Your array must have a non-zero length.  Only the first 624 integers
    //         * in the array are used; if the array is shorter than this then
    //         * integers are repeatedly used in a wrap-around fashion.
    //         */
    //
    //        public void setSeed(int[] array) {
    //            if (array.length == 0)
    //                throw new IllegalArgumentException("Array length must be greater than zero");
    //            int i, j, k;
    //            setSeed(19650218);
    //            i = 1;
    //            j = 0;
    //            k = (N > array.length ? N : array.length);
    //            for (; k != 0; k--) {
    //                mt[i] = (mt[i] ^ ((mt[i - 1] ^ (mt[i - 1] >>> 30)) * 1664525)) + array[j] + j; /* non linear */
    //                // mt[i] &= 0xffffffff; /* for WORDSIZE > 32 machines */
    //                i++;
    //                j++;
    //                if (i >= N) {
    //                    mt[0] = mt[N - 1];
    //                    i = 1;
    //                }
    //                if (j >= array.length)
    //                    j = 0;
    //            }
    //            for (k = N - 1; k != 0; k--) {
    //                mt[i] = (mt[i] ^ ((mt[i - 1] ^ (mt[i - 1] >>> 30)) * 1566083941)) - i; /* non linear */
    //                // mt[i] &= 0xffffffff; /* for WORDSIZE > 32 machines */
    //                i++;
    //                if (i >= N) {
    //                    mt[0] = mt[N - 1];
    //                    i = 1;
    //                }
    //            }
    //            mt[0] = 0x80000000; /* MSB is 1; assuring non-zero initial array */
    //        }
    //
    //        public int nextInt() {
    //            int y;
    //
    //            if (mti >= N) // generate N words at one time
    //            {
    //                int kk;
    //                final int[] mt = this.mt; // locals are slightly faster
    //                final int[] mag01 = this.mag01; // locals are slightly faster
    //
    //                for (kk = 0; kk < N - M; kk++) {
    //                    y = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
    //                    mt[kk] = mt[kk + M] ^ (y >>> 1) ^ mag01[y & 0x1];
    //                }
    //                for (; kk < N - 1; kk++) {
    //                    y = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
    //                    mt[kk] = mt[kk + (M - N)] ^ (y >>> 1) ^ mag01[y & 0x1];
    //                }
    //                y = (mt[N - 1] & UPPER_MASK) | (mt[0] & LOWER_MASK);
    //                mt[N - 1] = mt[M - 1] ^ (y >>> 1) ^ mag01[y & 0x1];
    //
    //                mti = 0;
    //            }
    //
    //            y = mt[mti++];
    //            y ^= y >>> 11; // TEMPERING_SHIFT_U(y)
    //            y ^= (y << 7) & TEMPERING_MASK_B; // TEMPERING_SHIFT_S(y)
    //            y ^= (y << 15) & TEMPERING_MASK_C; // TEMPERING_SHIFT_T(y)
    //            y ^= (y >>> 18); // TEMPERING_SHIFT_L(y)
    //
    //            return y;
    //        }
    //
    //        public short nextShort() {
    //            int y;
    //
    //            if (mti >= N) // generate N words at one time
    //            {
    //                int kk;
    //                final int[] mt = this.mt; // locals are slightly faster
    //                final int[] mag01 = this.mag01; // locals are slightly faster
    //
    //                for (kk = 0; kk < N - M; kk++) {
    //                    y = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
    //                    mt[kk] = mt[kk + M] ^ (y >>> 1) ^ mag01[y & 0x1];
    //                }
    //                for (; kk < N - 1; kk++) {
    //                    y = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
    //                    mt[kk] = mt[kk + (M - N)] ^ (y >>> 1) ^ mag01[y & 0x1];
    //                }
    //                y = (mt[N - 1] & UPPER_MASK) | (mt[0] & LOWER_MASK);
    //                mt[N - 1] = mt[M - 1] ^ (y >>> 1) ^ mag01[y & 0x1];
    //
    //                mti = 0;
    //            }
    //
    //            y = mt[mti++];
    //            y ^= y >>> 11; // TEMPERING_SHIFT_U(y)
    //            y ^= (y << 7) & TEMPERING_MASK_B; // TEMPERING_SHIFT_S(y)
    //            y ^= (y << 15) & TEMPERING_MASK_C; // TEMPERING_SHIFT_T(y)
    //            y ^= (y >>> 18); // TEMPERING_SHIFT_L(y)
    //
    //            return (short) (y >>> 16);
    //        }
    //
    //        public char nextChar() {
    //            int y;
    //
    //            if (mti >= N) // generate N words at one time
    //            {
    //                int kk;
    //                final int[] mt = this.mt; // locals are slightly faster
    //                final int[] mag01 = this.mag01; // locals are slightly faster
    //
    //                for (kk = 0; kk < N - M; kk++) {
    //                    y = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
    //                    mt[kk] = mt[kk + M] ^ (y >>> 1) ^ mag01[y & 0x1];
    //                }
    //                for (; kk < N - 1; kk++) {
    //                    y = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
    //                    mt[kk] = mt[kk + (M - N)] ^ (y >>> 1) ^ mag01[y & 0x1];
    //                }
    //                y = (mt[N - 1] & UPPER_MASK) | (mt[0] & LOWER_MASK);
    //                mt[N - 1] = mt[M - 1] ^ (y >>> 1) ^ mag01[y & 0x1];
    //
    //                mti = 0;
    //            }
    //
    //            y = mt[mti++];
    //            y ^= y >>> 11; // TEMPERING_SHIFT_U(y)
    //            y ^= (y << 7) & TEMPERING_MASK_B; // TEMPERING_SHIFT_S(y)
    //            y ^= (y << 15) & TEMPERING_MASK_C; // TEMPERING_SHIFT_T(y)
    //            y ^= (y >>> 18); // TEMPERING_SHIFT_L(y)
    //
    //            return (char) (y >>> 16);
    //        }
    //
    //        public boolean nextBoolean() {
    //            int y;
    //
    //            if (mti >= N) // generate N words at one time
    //            {
    //                int kk;
    //                final int[] mt = this.mt; // locals are slightly faster
    //                final int[] mag01 = this.mag01; // locals are slightly faster
    //
    //                for (kk = 0; kk < N - M; kk++) {
    //                    y = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
    //                    mt[kk] = mt[kk + M] ^ (y >>> 1) ^ mag01[y & 0x1];
    //                }
    //                for (; kk < N - 1; kk++) {
    //                    y = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
    //                    mt[kk] = mt[kk + (M - N)] ^ (y >>> 1) ^ mag01[y & 0x1];
    //                }
    //                y = (mt[N - 1] & UPPER_MASK) | (mt[0] & LOWER_MASK);
    //                mt[N - 1] = mt[M - 1] ^ (y >>> 1) ^ mag01[y & 0x1];
    //
    //                mti = 0;
    //            }
    //
    //            y = mt[mti++];
    //            y ^= y >>> 11; // TEMPERING_SHIFT_U(y)
    //            y ^= (y << 7) & TEMPERING_MASK_B; // TEMPERING_SHIFT_S(y)
    //            y ^= (y << 15) & TEMPERING_MASK_C; // TEMPERING_SHIFT_T(y)
    //            y ^= (y >>> 18); // TEMPERING_SHIFT_L(y)
    //
    //            return (boolean) ((y >>> 31) != 0);
    //        }
    //
    //        /** This generates a coin flip with a probability <tt>probability</tt>
    //         of returning true, else returning false.  <tt>probability</tt> must
    //         be between 0.0 and 1.0, inclusive.   Not as precise a random real
    //         event as nextBoolean(double), but twice as fast. To explicitly
    //         use this, remember you may need to cast to float first. */
    //
    //        public boolean nextBoolean(float probability) {
    //            int y;
    //
    //            if (probability < 0.0f || probability > 1.0f)
    //                throw new IllegalArgumentException("probability must be between 0.0 and 1.0 inclusive.");
    //            if (probability == 0.0f)
    //                return false; // fix half-open issues
    //            else if (probability == 1.0f)
    //                return true; // fix half-open issues
    //            if (mti >= N) // generate N words at one time
    //            {
    //                int kk;
    //                final int[] mt = this.mt; // locals are slightly faster
    //                final int[] mag01 = this.mag01; // locals are slightly faster
    //
    //                for (kk = 0; kk < N - M; kk++) {
    //                    y = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
    //                    mt[kk] = mt[kk + M] ^ (y >>> 1) ^ mag01[y & 0x1];
    //                }
    //                for (; kk < N - 1; kk++) {
    //                    y = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
    //                    mt[kk] = mt[kk + (M - N)] ^ (y >>> 1) ^ mag01[y & 0x1];
    //                }
    //                y = (mt[N - 1] & UPPER_MASK) | (mt[0] & LOWER_MASK);
    //                mt[N - 1] = mt[M - 1] ^ (y >>> 1) ^ mag01[y & 0x1];
    //
    //                mti = 0;
    //            }
    //
    //            y = mt[mti++];
    //            y ^= y >>> 11; // TEMPERING_SHIFT_U(y)
    //            y ^= (y << 7) & TEMPERING_MASK_B; // TEMPERING_SHIFT_S(y)
    //            y ^= (y << 15) & TEMPERING_MASK_C; // TEMPERING_SHIFT_T(y)
    //            y ^= (y >>> 18); // TEMPERING_SHIFT_L(y)
    //
    //            return (y >>> 8) / ((float) (1 << 24)) < probability;
    //        }
    //
    //        /** This generates a coin flip with a probability <tt>probability</tt>
    //         of returning true, else returning false.  <tt>probability</tt> must
    //         be between 0.0 and 1.0, inclusive. */
    //
    //        public boolean nextBoolean(double probability) {
    //            int y;
    //            int z;
    //
    //            if (probability < 0.0 || probability > 1.0)
    //                throw new IllegalArgumentException("probability must be between 0.0 and 1.0 inclusive.");
    //            if (probability == 0.0)
    //                return false; // fix half-open issues
    //            else if (probability == 1.0)
    //                return true; // fix half-open issues
    //            if (mti >= N) // generate N words at one time
    //            {
    //                int kk;
    //                final int[] mt = this.mt; // locals are slightly faster
    //                final int[] mag01 = this.mag01; // locals are slightly faster
    //
    //                for (kk = 0; kk < N - M; kk++) {
    //                    y = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
    //                    mt[kk] = mt[kk + M] ^ (y >>> 1) ^ mag01[y & 0x1];
    //                }
    //                for (; kk < N - 1; kk++) {
    //                    y = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
    //                    mt[kk] = mt[kk + (M - N)] ^ (y >>> 1) ^ mag01[y & 0x1];
    //                }
    //                y = (mt[N - 1] & UPPER_MASK) | (mt[0] & LOWER_MASK);
    //                mt[N - 1] = mt[M - 1] ^ (y >>> 1) ^ mag01[y & 0x1];
    //
    //                mti = 0;
    //            }
    //
    //            y = mt[mti++];
    //            y ^= y >>> 11; // TEMPERING_SHIFT_U(y)
    //            y ^= (y << 7) & TEMPERING_MASK_B; // TEMPERING_SHIFT_S(y)
    //            y ^= (y << 15) & TEMPERING_MASK_C; // TEMPERING_SHIFT_T(y)
    //            y ^= (y >>> 18); // TEMPERING_SHIFT_L(y)
    //
    //            if (mti >= N) // generate N words at one time
    //            {
    //                int kk;
    //                final int[] mt = this.mt; // locals are slightly faster
    //                final int[] mag01 = this.mag01; // locals are slightly faster
    //
    //                for (kk = 0; kk < N - M; kk++) {
    //                    z = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
    //                    mt[kk] = mt[kk + M] ^ (z >>> 1) ^ mag01[z & 0x1];
    //                }
    //                for (; kk < N - 1; kk++) {
    //                    z = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
    //                    mt[kk] = mt[kk + (M - N)] ^ (z >>> 1) ^ mag01[z & 0x1];
    //                }
    //                z = (mt[N - 1] & UPPER_MASK) | (mt[0] & LOWER_MASK);
    //                mt[N - 1] = mt[M - 1] ^ (z >>> 1) ^ mag01[z & 0x1];
    //
    //                mti = 0;
    //            }
    //
    //            z = mt[mti++];
    //            z ^= z >>> 11; // TEMPERING_SHIFT_U(z)
    //            z ^= (z << 7) & TEMPERING_MASK_B; // TEMPERING_SHIFT_S(z)
    //            z ^= (z << 15) & TEMPERING_MASK_C; // TEMPERING_SHIFT_T(z)
    //            z ^= (z >>> 18); // TEMPERING_SHIFT_L(z)
    //
    //            /* derived from nextDouble documentation in jdk 1.2 docs, see top */
    //            return ((((long) (y >>> 6)) << 27) + (z >>> 5)) / (double) (1L << 53) < probability;
    //        }
    //
    //        public byte nextByte() {
    //            int y;
    //
    //            if (mti >= N) // generate N words at one time
    //            {
    //                int kk;
    //                final int[] mt = this.mt; // locals are slightly faster
    //                final int[] mag01 = this.mag01; // locals are slightly faster
    //
    //                for (kk = 0; kk < N - M; kk++) {
    //                    y = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
    //                    mt[kk] = mt[kk + M] ^ (y >>> 1) ^ mag01[y & 0x1];
    //                }
    //                for (; kk < N - 1; kk++) {
    //                    y = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
    //                    mt[kk] = mt[kk + (M - N)] ^ (y >>> 1) ^ mag01[y & 0x1];
    //                }
    //                y = (mt[N - 1] & UPPER_MASK) | (mt[0] & LOWER_MASK);
    //                mt[N - 1] = mt[M - 1] ^ (y >>> 1) ^ mag01[y & 0x1];
    //
    //                mti = 0;
    //            }
    //
    //            y = mt[mti++];
    //            y ^= y >>> 11; // TEMPERING_SHIFT_U(y)
    //            y ^= (y << 7) & TEMPERING_MASK_B; // TEMPERING_SHIFT_S(y)
    //            y ^= (y << 15) & TEMPERING_MASK_C; // TEMPERING_SHIFT_T(y)
    //            y ^= (y >>> 18); // TEMPERING_SHIFT_L(y)
    //
    //            return (byte) (y >>> 24);
    //        }
    //
    //        public void nextBytes(byte[] bytes) {
    //            int y;
    //
    //            for (int x = 0; x < bytes.length; x++) {
    //                if (mti >= N) // generate N words at one time
    //                {
    //                    int kk;
    //                    final int[] mt = this.mt; // locals are slightly faster
    //                    final int[] mag01 = this.mag01; // locals are slightly faster
    //
    //                    for (kk = 0; kk < N - M; kk++) {
    //                        y = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
    //                        mt[kk] = mt[kk + M] ^ (y >>> 1) ^ mag01[y & 0x1];
    //                    }
    //                    for (; kk < N - 1; kk++) {
    //                        y = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
    //                        mt[kk] = mt[kk + (M - N)] ^ (y >>> 1) ^ mag01[y & 0x1];
    //                    }
    //                    y = (mt[N - 1] & UPPER_MASK) | (mt[0] & LOWER_MASK);
    //                    mt[N - 1] = mt[M - 1] ^ (y >>> 1) ^ mag01[y & 0x1];
    //
    //                    mti = 0;
    //                }
    //
    //                y = mt[mti++];
    //                y ^= y >>> 11; // TEMPERING_SHIFT_U(y)
    //                y ^= (y << 7) & TEMPERING_MASK_B; // TEMPERING_SHIFT_S(y)
    //                y ^= (y << 15) & TEMPERING_MASK_C; // TEMPERING_SHIFT_T(y)
    //                y ^= (y >>> 18); // TEMPERING_SHIFT_L(y)
    //
    //                bytes[x] = (byte) (y >>> 24);
    //            }
    //        }
    //
    //        /** Returns a long drawn uniformly from 0 to n-1.  Suffice it to say,
    //         n must be greater than 0, or an IllegalArgumentException is raised. */
    //
    //        public long nextLong() {
    //            int y;
    //            int z;
    //
    //            if (mti >= N) // generate N words at one time
    //            {
    //                int kk;
    //                final int[] mt = this.mt; // locals are slightly faster
    //                final int[] mag01 = this.mag01; // locals are slightly faster
    //
    //                for (kk = 0; kk < N - M; kk++) {
    //                    y = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
    //                    mt[kk] = mt[kk + M] ^ (y >>> 1) ^ mag01[y & 0x1];
    //                }
    //                for (; kk < N - 1; kk++) {
    //                    y = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
    //                    mt[kk] = mt[kk + (M - N)] ^ (y >>> 1) ^ mag01[y & 0x1];
    //                }
    //                y = (mt[N - 1] & UPPER_MASK) | (mt[0] & LOWER_MASK);
    //                mt[N - 1] = mt[M - 1] ^ (y >>> 1) ^ mag01[y & 0x1];
    //
    //                mti = 0;
    //            }
    //
    //            y = mt[mti++];
    //            y ^= y >>> 11; // TEMPERING_SHIFT_U(y)
    //            y ^= (y << 7) & TEMPERING_MASK_B; // TEMPERING_SHIFT_S(y)
    //            y ^= (y << 15) & TEMPERING_MASK_C; // TEMPERING_SHIFT_T(y)
    //            y ^= (y >>> 18); // TEMPERING_SHIFT_L(y)
    //
    //            if (mti >= N) // generate N words at one time
    //            {
    //                int kk;
    //                final int[] mt = this.mt; // locals are slightly faster
    //                final int[] mag01 = this.mag01; // locals are slightly faster
    //
    //                for (kk = 0; kk < N - M; kk++) {
    //                    z = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
    //                    mt[kk] = mt[kk + M] ^ (z >>> 1) ^ mag01[z & 0x1];
    //                }
    //                for (; kk < N - 1; kk++) {
    //                    z = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
    //                    mt[kk] = mt[kk + (M - N)] ^ (z >>> 1) ^ mag01[z & 0x1];
    //                }
    //                z = (mt[N - 1] & UPPER_MASK) | (mt[0] & LOWER_MASK);
    //                mt[N - 1] = mt[M - 1] ^ (z >>> 1) ^ mag01[z & 0x1];
    //
    //                mti = 0;
    //            }
    //
    //            z = mt[mti++];
    //            z ^= z >>> 11; // TEMPERING_SHIFT_U(z)
    //            z ^= (z << 7) & TEMPERING_MASK_B; // TEMPERING_SHIFT_S(z)
    //            z ^= (z << 15) & TEMPERING_MASK_C; // TEMPERING_SHIFT_T(z)
    //            z ^= (z >>> 18); // TEMPERING_SHIFT_L(z)
    //
    //            return (((long) y) << 32) + (long) z;
    //        }
    //
    //        /** Returns a long drawn uniformly from 0 to n-1.  Suffice it to say,
    //         n must be &gt; 0, or an IllegalArgumentException is raised. */
    //        public long nextLong(long n) {
    //            if (n <= 0)
    //                throw new IllegalArgumentException("n must be positive, got: " + n);
    //
    //            long bits, val;
    //            do {
    //                int y;
    //                int z;
    //
    //                if (mti >= N) // generate N words at one time
    //                {
    //                    int kk;
    //                    final int[] mt = this.mt; // locals are slightly faster
    //                    final int[] mag01 = this.mag01; // locals are slightly faster
    //
    //                    for (kk = 0; kk < N - M; kk++) {
    //                        y = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
    //                        mt[kk] = mt[kk + M] ^ (y >>> 1) ^ mag01[y & 0x1];
    //                    }
    //                    for (; kk < N - 1; kk++) {
    //                        y = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
    //                        mt[kk] = mt[kk + (M - N)] ^ (y >>> 1) ^ mag01[y & 0x1];
    //                    }
    //                    y = (mt[N - 1] & UPPER_MASK) | (mt[0] & LOWER_MASK);
    //                    mt[N - 1] = mt[M - 1] ^ (y >>> 1) ^ mag01[y & 0x1];
    //
    //                    mti = 0;
    //                }
    //
    //                y = mt[mti++];
    //                y ^= y >>> 11; // TEMPERING_SHIFT_U(y)
    //                y ^= (y << 7) & TEMPERING_MASK_B; // TEMPERING_SHIFT_S(y)
    //                y ^= (y << 15) & TEMPERING_MASK_C; // TEMPERING_SHIFT_T(y)
    //                y ^= (y >>> 18); // TEMPERING_SHIFT_L(y)
    //
    //                if (mti >= N) // generate N words at one time
    //                {
    //                    int kk;
    //                    final int[] mt = this.mt; // locals are slightly faster
    //                    final int[] mag01 = this.mag01; // locals are slightly faster
    //
    //                    for (kk = 0; kk < N - M; kk++) {
    //                        z = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
    //                        mt[kk] = mt[kk + M] ^ (z >>> 1) ^ mag01[z & 0x1];
    //                    }
    //                    for (; kk < N - 1; kk++) {
    //                        z = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
    //                        mt[kk] = mt[kk + (M - N)] ^ (z >>> 1) ^ mag01[z & 0x1];
    //                    }
    //                    z = (mt[N - 1] & UPPER_MASK) | (mt[0] & LOWER_MASK);
    //                    mt[N - 1] = mt[M - 1] ^ (z >>> 1) ^ mag01[z & 0x1];
    //
    //                    mti = 0;
    //                }
    //
    //                z = mt[mti++];
    //                z ^= z >>> 11; // TEMPERING_SHIFT_U(z)
    //                z ^= (z << 7) & TEMPERING_MASK_B; // TEMPERING_SHIFT_S(z)
    //                z ^= (z << 15) & TEMPERING_MASK_C; // TEMPERING_SHIFT_T(z)
    //                z ^= (z >>> 18); // TEMPERING_SHIFT_L(z)
    //
    //                bits = (((((long) y) << 32) + (long) z) >>> 1);
    //                val = bits % n;
    //            } while (bits - val + (n - 1) < 0);
    //            return val;
    //        }
    //
    //        /** Returns a random double in the half-open range from [0.0,1.0).  Thus 0.0 is a valid
    //         result but 1.0 is not. */
    //        public double nextDouble() {
    //            int y;
    //            int z;
    //
    //            if (mti >= N) // generate N words at one time
    //            {
    //                int kk;
    //                final int[] mt = this.mt; // locals are slightly faster
    //                final int[] mag01 = this.mag01; // locals are slightly faster
    //
    //                for (kk = 0; kk < N - M; kk++) {
    //                    y = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
    //                    mt[kk] = mt[kk + M] ^ (y >>> 1) ^ mag01[y & 0x1];
    //                }
    //                for (; kk < N - 1; kk++) {
    //                    y = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
    //                    mt[kk] = mt[kk + (M - N)] ^ (y >>> 1) ^ mag01[y & 0x1];
    //                }
    //                y = (mt[N - 1] & UPPER_MASK) | (mt[0] & LOWER_MASK);
    //                mt[N - 1] = mt[M - 1] ^ (y >>> 1) ^ mag01[y & 0x1];
    //
    //                mti = 0;
    //            }
    //
    //            y = mt[mti++];
    //            y ^= y >>> 11; // TEMPERING_SHIFT_U(y)
    //            y ^= (y << 7) & TEMPERING_MASK_B; // TEMPERING_SHIFT_S(y)
    //            y ^= (y << 15) & TEMPERING_MASK_C; // TEMPERING_SHIFT_T(y)
    //            y ^= (y >>> 18); // TEMPERING_SHIFT_L(y)
    //
    //            if (mti >= N) // generate N words at one time
    //            {
    //                int kk;
    //                final int[] mt = this.mt; // locals are slightly faster
    //                final int[] mag01 = this.mag01; // locals are slightly faster
    //
    //                for (kk = 0; kk < N - M; kk++) {
    //                    z = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
    //                    mt[kk] = mt[kk + M] ^ (z >>> 1) ^ mag01[z & 0x1];
    //                }
    //                for (; kk < N - 1; kk++) {
    //                    z = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
    //                    mt[kk] = mt[kk + (M - N)] ^ (z >>> 1) ^ mag01[z & 0x1];
    //                }
    //                z = (mt[N - 1] & UPPER_MASK) | (mt[0] & LOWER_MASK);
    //                mt[N - 1] = mt[M - 1] ^ (z >>> 1) ^ mag01[z & 0x1];
    //
    //                mti = 0;
    //            }
    //
    //            z = mt[mti++];
    //            z ^= z >>> 11; // TEMPERING_SHIFT_U(z)
    //            z ^= (z << 7) & TEMPERING_MASK_B; // TEMPERING_SHIFT_S(z)
    //            z ^= (z << 15) & TEMPERING_MASK_C; // TEMPERING_SHIFT_T(z)
    //            z ^= (z >>> 18); // TEMPERING_SHIFT_L(z)
    //
    //            /* derived from nextDouble documentation in jdk 1.2 docs, see top */
    //            return ((((long) (y >>> 6)) << 27) + (z >>> 5)) / (double) (1L << 53);
    //        }
    //
    //        /** Returns a double in the range from 0.0 to 1.0, possibly inclusive of 0.0 and 1.0 themselves.  Thus:
    //
    //         <table border=0>
    //         <tr><th>Expression</th><th>Interval</th></tr>
    //         <tr><td>nextDouble(false, false)</td><td>(0.0, 1.0)</td></tr>
    //         <tr><td>nextDouble(true, false)</td><td>[0.0, 1.0)</td></tr>
    //         <tr><td>nextDouble(false, true)</td><td>(0.0, 1.0]</td></tr>
    //         <tr><td>nextDouble(true, true)</td><td>[0.0, 1.0]</td></tr>
    //         <caption>Table of intervals</caption>
    //         </table>
    //
    //         <p>This version preserves all possible random values in the double range.
    //         */
    //        public double nextDouble(boolean includeZero, boolean includeOne) {
    //            double d = 0.0;
    //            do {
    //                d = nextDouble(); // grab a value, initially from half-open [0.0, 1.0)
    //                if (includeOne && nextBoolean())
    //                    d += 1.0; // if includeOne, with 1/2 probability, push to [1.0, 2.0)
    //            } while ((d > 1.0) || // everything above 1.0 is always invalid
    //                    (!includeZero && d == 0.0)); // if we're not including zero, 0.0 is invalid
    //            return d;
    //        }
    //
    //        /**
    //         Clears the internal gaussian variable from the RNG.  You only need to do this
    //         in the rare case that you need to guarantee that two RNGs have identical internal
    //         state.  Otherwise, disregard this method.  See stateEquals(other).
    //         */
    //        public void clearGaussian() {
    //            __haveNextNextGaussian = false;
    //        }
    //
    //        public double nextGaussian() {
    //            if (__haveNextNextGaussian) {
    //                __haveNextNextGaussian = false;
    //                return __nextNextGaussian;
    //            } else {
    //                double v1, v2, s;
    //                do {
    //                    int y;
    //                    int z;
    //                    int a;
    //                    int b;
    //
    //                    if (mti >= N) // generate N words at one time
    //                    {
    //                        int kk;
    //                        final int[] mt = this.mt; // locals are slightly faster
    //                        final int[] mag01 = this.mag01; // locals are slightly faster
    //
    //                        for (kk = 0; kk < N - M; kk++) {
    //                            y = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
    //                            mt[kk] = mt[kk + M] ^ (y >>> 1) ^ mag01[y & 0x1];
    //                        }
    //                        for (; kk < N - 1; kk++) {
    //                            y = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
    //                            mt[kk] = mt[kk + (M - N)] ^ (y >>> 1) ^ mag01[y & 0x1];
    //                        }
    //                        y = (mt[N - 1] & UPPER_MASK) | (mt[0] & LOWER_MASK);
    //                        mt[N - 1] = mt[M - 1] ^ (y >>> 1) ^ mag01[y & 0x1];
    //
    //                        mti = 0;
    //                    }
    //
    //                    y = mt[mti++];
    //                    y ^= y >>> 11; // TEMPERING_SHIFT_U(y)
    //                    y ^= (y << 7) & TEMPERING_MASK_B; // TEMPERING_SHIFT_S(y)
    //                    y ^= (y << 15) & TEMPERING_MASK_C; // TEMPERING_SHIFT_T(y)
    //                    y ^= (y >>> 18); // TEMPERING_SHIFT_L(y)
    //
    //                    if (mti >= N) // generate N words at one time
    //                    {
    //                        int kk;
    //                        final int[] mt = this.mt; // locals are slightly faster
    //                        final int[] mag01 = this.mag01; // locals are slightly faster
    //
    //                        for (kk = 0; kk < N - M; kk++) {
    //                            z = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
    //                            mt[kk] = mt[kk + M] ^ (z >>> 1) ^ mag01[z & 0x1];
    //                        }
    //                        for (; kk < N - 1; kk++) {
    //                            z = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
    //                            mt[kk] = mt[kk + (M - N)] ^ (z >>> 1) ^ mag01[z & 0x1];
    //                        }
    //                        z = (mt[N - 1] & UPPER_MASK) | (mt[0] & LOWER_MASK);
    //                        mt[N - 1] = mt[M - 1] ^ (z >>> 1) ^ mag01[z & 0x1];
    //
    //                        mti = 0;
    //                    }
    //
    //                    z = mt[mti++];
    //                    z ^= z >>> 11; // TEMPERING_SHIFT_U(z)
    //                    z ^= (z << 7) & TEMPERING_MASK_B; // TEMPERING_SHIFT_S(z)
    //                    z ^= (z << 15) & TEMPERING_MASK_C; // TEMPERING_SHIFT_T(z)
    //                    z ^= (z >>> 18); // TEMPERING_SHIFT_L(z)
    //
    //                    if (mti >= N) // generate N words at one time
    //                    {
    //                        int kk;
    //                        final int[] mt = this.mt; // locals are slightly faster
    //                        final int[] mag01 = this.mag01; // locals are slightly faster
    //
    //                        for (kk = 0; kk < N - M; kk++) {
    //                            a = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
    //                            mt[kk] = mt[kk + M] ^ (a >>> 1) ^ mag01[a & 0x1];
    //                        }
    //                        for (; kk < N - 1; kk++) {
    //                            a = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
    //                            mt[kk] = mt[kk + (M - N)] ^ (a >>> 1) ^ mag01[a & 0x1];
    //                        }
    //                        a = (mt[N - 1] & UPPER_MASK) | (mt[0] & LOWER_MASK);
    //                        mt[N - 1] = mt[M - 1] ^ (a >>> 1) ^ mag01[a & 0x1];
    //
    //                        mti = 0;
    //                    }
    //
    //                    a = mt[mti++];
    //                    a ^= a >>> 11; // TEMPERING_SHIFT_U(a)
    //                    a ^= (a << 7) & TEMPERING_MASK_B; // TEMPERING_SHIFT_S(a)
    //                    a ^= (a << 15) & TEMPERING_MASK_C; // TEMPERING_SHIFT_T(a)
    //                    a ^= (a >>> 18); // TEMPERING_SHIFT_L(a)
    //
    //                    if (mti >= N) // generate N words at one time
    //                    {
    //                        int kk;
    //                        final int[] mt = this.mt; // locals are slightly faster
    //                        final int[] mag01 = this.mag01; // locals are slightly faster
    //
    //                        for (kk = 0; kk < N - M; kk++) {
    //                            b = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
    //                            mt[kk] = mt[kk + M] ^ (b >>> 1) ^ mag01[b & 0x1];
    //                        }
    //                        for (; kk < N - 1; kk++) {
    //                            b = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
    //                            mt[kk] = mt[kk + (M - N)] ^ (b >>> 1) ^ mag01[b & 0x1];
    //                        }
    //                        b = (mt[N - 1] & UPPER_MASK) | (mt[0] & LOWER_MASK);
    //                        mt[N - 1] = mt[M - 1] ^ (b >>> 1) ^ mag01[b & 0x1];
    //
    //                        mti = 0;
    //                    }
    //
    //                    b = mt[mti++];
    //                    b ^= b >>> 11; // TEMPERING_SHIFT_U(b)
    //                    b ^= (b << 7) & TEMPERING_MASK_B; // TEMPERING_SHIFT_S(b)
    //                    b ^= (b << 15) & TEMPERING_MASK_C; // TEMPERING_SHIFT_T(b)
    //                    b ^= (b >>> 18); // TEMPERING_SHIFT_L(b)
    //
    //                    /* derived from nextDouble documentation in jdk 1.2 docs, see top */
    //                    v1 = 2 * (((((long) (y >>> 6)) << 27) + (z >>> 5)) / (double) (1L << 53)) - 1;
    //                    v2 = 2 * (((((long) (a >>> 6)) << 27) + (b >>> 5)) / (double) (1L << 53)) - 1;
    //                    s = v1 * v1 + v2 * v2;
    //                } while (s >= 1 || s == 0);
    //                double multiplier = StrictMath.sqrt(-2 * StrictMath.log(s) / s);
    //                __nextNextGaussian = v2 * multiplier;
    //                __haveNextNextGaussian = true;
    //                return v1 * multiplier;
    //            }
    //        }
    //
    //        /** Returns a random float in the half-open range from [0.0f,1.0f).  Thus 0.0f is a valid
    //         result but 1.0f is not. */
    //        public float nextFloat() {
    //            int y;
    //
    //            if (mti >= N) // generate N words at one time
    //            {
    //                int kk;
    //                final int[] mt = this.mt; // locals are slightly faster
    //                final int[] mag01 = this.mag01; // locals are slightly faster
    //
    //                for (kk = 0; kk < N - M; kk++) {
    //                    y = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
    //                    mt[kk] = mt[kk + M] ^ (y >>> 1) ^ mag01[y & 0x1];
    //                }
    //                for (; kk < N - 1; kk++) {
    //                    y = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
    //                    mt[kk] = mt[kk + (M - N)] ^ (y >>> 1) ^ mag01[y & 0x1];
    //                }
    //                y = (mt[N - 1] & UPPER_MASK) | (mt[0] & LOWER_MASK);
    //                mt[N - 1] = mt[M - 1] ^ (y >>> 1) ^ mag01[y & 0x1];
    //
    //                mti = 0;
    //            }
    //
    //            y = mt[mti++];
    //            y ^= y >>> 11; // TEMPERING_SHIFT_U(y)
    //            y ^= (y << 7) & TEMPERING_MASK_B; // TEMPERING_SHIFT_S(y)
    //            y ^= (y << 15) & TEMPERING_MASK_C; // TEMPERING_SHIFT_T(y)
    //            y ^= (y >>> 18); // TEMPERING_SHIFT_L(y)
    //
    //            return (y >>> 8) / ((float) (1 << 24));
    //        }
    //
    //        /** Returns a float in the range from 0.0f to 1.0f, possibly inclusive of 0.0f and 1.0f themselves.  Thus:
    //
    //         <table border=0>
    //         <tr><th>Expression</th><th>Interval</th></tr>
    //         <tr><td>nextFloat(false, false)</td><td>(0.0f, 1.0f)</td></tr>
    //         <tr><td>nextFloat(true, false)</td><td>[0.0f, 1.0f)</td></tr>
    //         <tr><td>nextFloat(false, true)</td><td>(0.0f, 1.0f]</td></tr>
    //         <tr><td>nextFloat(true, true)</td><td>[0.0f, 1.0f]</td></tr>
    //         <caption>Table of intervals</caption>
    //         </table>
    //
    //         <p>This version preserves all possible random values in the float range.
    //         */
    //        public float nextFloat(boolean includeZero, boolean includeOne) {
    //            float d = 0.0f;
    //            do {
    //                d = nextFloat(); // grab a value, initially from half-open [0.0f, 1.0f)
    //                if (includeOne && nextBoolean())
    //                    d += 1.0f; // if includeOne, with 1/2 probability, push to [1.0f, 2.0f)
    //            } while ((d > 1.0f) || // everything above 1.0f is always invalid
    //                    (!includeZero && d == 0.0f)); // if we're not including zero, 0.0f is invalid
    //            return d;
    //        }
    //
    //        /** Returns an integer drawn uniformly from 0 to n-1.  Suffice it to say,
    //         n must be &gt; 0, or an IllegalArgumentException is raised. */
    //        public int nextInt(int n) {
    //            if (n <= 0)
    //                throw new IllegalArgumentException("n must be positive, got: " + n);
    //
    //            if ((n & -n) == n) // i.e., n is a power of 2
    //            {
    //                int y;
    //
    //                if (mti >= N) // generate N words at one time
    //                {
    //                    int kk;
    //                    final int[] mt = this.mt; // locals are slightly faster
    //                    final int[] mag01 = this.mag01; // locals are slightly faster
    //
    //                    for (kk = 0; kk < N - M; kk++) {
    //                        y = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
    //                        mt[kk] = mt[kk + M] ^ (y >>> 1) ^ mag01[y & 0x1];
    //                    }
    //                    for (; kk < N - 1; kk++) {
    //                        y = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
    //                        mt[kk] = mt[kk + (M - N)] ^ (y >>> 1) ^ mag01[y & 0x1];
    //                    }
    //                    y = (mt[N - 1] & UPPER_MASK) | (mt[0] & LOWER_MASK);
    //                    mt[N - 1] = mt[M - 1] ^ (y >>> 1) ^ mag01[y & 0x1];
    //
    //                    mti = 0;
    //                }
    //
    //                y = mt[mti++];
    //                y ^= y >>> 11; // TEMPERING_SHIFT_U(y)
    //                y ^= (y << 7) & TEMPERING_MASK_B; // TEMPERING_SHIFT_S(y)
    //                y ^= (y << 15) & TEMPERING_MASK_C; // TEMPERING_SHIFT_T(y)
    //                y ^= (y >>> 18); // TEMPERING_SHIFT_L(y)
    //
    //                return (int) ((n * (long) (y >>> 1)) >> 31);
    //            }
    //
    //            int bits, val;
    //            do {
    //                int y;
    //
    //                if (mti >= N) // generate N words at one time
    //                {
    //                    int kk;
    //                    final int[] mt = this.mt; // locals are slightly faster
    //                    final int[] mag01 = this.mag01; // locals are slightly faster
    //
    //                    for (kk = 0; kk < N - M; kk++) {
    //                        y = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
    //                        mt[kk] = mt[kk + M] ^ (y >>> 1) ^ mag01[y & 0x1];
    //                    }
    //                    for (; kk < N - 1; kk++) {
    //                        y = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
    //                        mt[kk] = mt[kk + (M - N)] ^ (y >>> 1) ^ mag01[y & 0x1];
    //                    }
    //                    y = (mt[N - 1] & UPPER_MASK) | (mt[0] & LOWER_MASK);
    //                    mt[N - 1] = mt[M - 1] ^ (y >>> 1) ^ mag01[y & 0x1];
    //
    //                    mti = 0;
    //                }
    //
    //                y = mt[mti++];
    //                y ^= y >>> 11; // TEMPERING_SHIFT_U(y)
    //                y ^= (y << 7) & TEMPERING_MASK_B; // TEMPERING_SHIFT_S(y)
    //                y ^= (y << 15) & TEMPERING_MASK_C; // TEMPERING_SHIFT_T(y)
    //                y ^= (y >>> 18); // TEMPERING_SHIFT_L(y)
    //
    //                bits = (y >>> 1);
    //                val = bits % n;
    //            } while (bits - val + (n - 1) < 0);
    //            return val;
    //        }
    //
    //        /**
    //         * Tests the code.
    //         */
    //        public static void main(String args[]) {
    //            int j;
    //
    //            MersenneTwisterFast r;
    //
    //            // CORRECTNESS TEST
    //            // COMPARE WITH http://www.math.keio.ac.jp/matumoto/CODES/MT2002/mt19937ar.out
    //
    //            r = new MersenneTwisterFast(new int[] { 0x123, 0x234, 0x345, 0x456 });
    //            System.out.println("Output of MersenneTwisterFast with new (2002/1/26) seeding mechanism");
    //            for (j = 0; j < 1000; j++) {
    //                // first, convert the int from signed to "unsigned"
    //                long l = (long) r.nextInt();
    //                if (l < 0)
    //                    l += 4294967296L; // max int value
    //                String s = String.valueOf(l);
    //                while (s.length() < 10)
    //                    s = " " + s; // buffer
    //                System.out.print(s + " ");
    //                if (j % 5 == 4)
    //                    System.out.println();
    //            }
    //
    //            // SPEED TEST
    //
    //            final long SEED = 4357;
    //
    //            int xx;
    //            long ms;
    //            System.out.println("\nTime to test grabbing 100000000 ints");
    //
    //            Random rr = new Random(SEED);
    //            xx = 0;
    //            ms = System.currentTimeMillis();
    //            for (j = 0; j < 100000000; j++)
    //                xx += rr.nextInt();
    //            System.out
    //                    .println("java.util.Random: " + (System.currentTimeMillis() - ms) + "          Ignore this: " + xx);
    //
    //            r = new MersenneTwisterFast(SEED);
    //            ms = System.currentTimeMillis();
    //            xx = 0;
    //            for (j = 0; j < 100000000; j++)
    //                xx += r.nextInt();
    //            System.out.println(
    //                    "Mersenne Twister Fast: " + (System.currentTimeMillis() - ms) + "          Ignore this: " + xx);
    //
    //            // TEST TO COMPARE TYPE CONVERSION BETWEEN
    //            // MersenneTwisterFast.java AND MersenneTwister.java
    //
    //            System.out.println("\nGrab the first 1000 booleans");
    //            r = new MersenneTwisterFast(SEED);
    //            for (j = 0; j < 1000; j++) {
    //                System.out.print(r.nextBoolean() + " ");
    //                if (j % 8 == 7)
    //                    System.out.println();
    //            }
    //            if (!(j % 8 == 7))
    //                System.out.println();
    //
    //            System.out.println("\nGrab 1000 booleans of increasing probability using nextBoolean(double)");
    //            r = new MersenneTwisterFast(SEED);
    //            for (j = 0; j < 1000; j++) {
    //                System.out.print(r.nextBoolean((double) (j / 999.0)) + " ");
    //                if (j % 8 == 7)
    //                    System.out.println();
    //            }
    //            if (!(j % 8 == 7))
    //                System.out.println();
    //
    //            System.out.println("\nGrab 1000 booleans of increasing probability using nextBoolean(float)");
    //            r = new MersenneTwisterFast(SEED);
    //            for (j = 0; j < 1000; j++) {
    //                System.out.print(r.nextBoolean((float) (j / 999.0f)) + " ");
    //                if (j % 8 == 7)
    //                    System.out.println();
    //            }
    //            if (!(j % 8 == 7))
    //                System.out.println();
    //
    //            byte[] bytes = new byte[1000];
    //            System.out.println("\nGrab the first 1000 bytes using nextBytes");
    //            r = new MersenneTwisterFast(SEED);
    //            r.nextBytes(bytes);
    //            for (j = 0; j < 1000; j++) {
    //                System.out.print(bytes[j] + " ");
    //                if (j % 16 == 15)
    //                    System.out.println();
    //            }
    //            if (!(j % 16 == 15))
    //                System.out.println();
    //
    //            byte b;
    //            System.out.println("\nGrab the first 1000 bytes -- must be same as nextBytes");
    //            r = new MersenneTwisterFast(SEED);
    //            for (j = 0; j < 1000; j++) {
    //                System.out.print((b = r.nextByte()) + " ");
    //                if (b != bytes[j])
    //                    System.out.print("BAD ");
    //                if (j % 16 == 15)
    //                    System.out.println();
    //            }
    //            if (!(j % 16 == 15))
    //                System.out.println();
    //
    //            System.out.println("\nGrab the first 1000 shorts");
    //            r = new MersenneTwisterFast(SEED);
    //            for (j = 0; j < 1000; j++) {
    //                System.out.print(r.nextShort() + " ");
    //                if (j % 8 == 7)
    //                    System.out.println();
    //            }
    //            if (!(j % 8 == 7))
    //                System.out.println();
    //
    //            System.out.println("\nGrab the first 1000 ints");
    //            r = new MersenneTwisterFast(SEED);
    //            for (j = 0; j < 1000; j++) {
    //                System.out.print(r.nextInt() + " ");
    //                if (j % 4 == 3)
    //                    System.out.println();
    //            }
    //            if (!(j % 4 == 3))
    //                System.out.println();
    //
    //            System.out.println("\nGrab the first 1000 ints of different sizes");
    //            r = new MersenneTwisterFast(SEED);
    //            int max = 1;
    //            for (j = 0; j < 1000; j++) {
    //                System.out.print(r.nextInt(max) + " ");
    //                max *= 2;
    //                if (max <= 0)
    //                    max = 1;
    //                if (j % 4 == 3)
    //                    System.out.println();
    //            }
    //            if (!(j % 4 == 3))
    //                System.out.println();
    //
    //            System.out.println("\nGrab the first 1000 longs");
    //            r = new MersenneTwisterFast(SEED);
    //            for (j = 0; j < 1000; j++) {
    //                System.out.print(r.nextLong() + " ");
    //                if (j % 3 == 2)
    //                    System.out.println();
    //            }
    //            if (!(j % 3 == 2))
    //                System.out.println();
    //
    //            System.out.println("\nGrab the first 1000 longs of different sizes");
    //            r = new MersenneTwisterFast(SEED);
    //            long max2 = 1;
    //            for (j = 0; j < 1000; j++) {
    //                System.out.print(r.nextLong(max2) + " ");
    //                max2 *= 2;
    //                if (max2 <= 0)
    //                    max2 = 1;
    //                if (j % 4 == 3)
    //                    System.out.println();
    //            }
    //            if (!(j % 4 == 3))
    //                System.out.println();
    //
    //            System.out.println("\nGrab the first 1000 floats");
    //            r = new MersenneTwisterFast(SEED);
    //            for (j = 0; j < 1000; j++) {
    //                System.out.print(r.nextFloat() + " ");
    //                if (j % 4 == 3)
    //                    System.out.println();
    //            }
    //            if (!(j % 4 == 3))
    //                System.out.println();
    //
    //            System.out.println("\nGrab the first 1000 doubles");
    //            r = new MersenneTwisterFast(SEED);
    //            for (j = 0; j < 1000; j++) {
    //                System.out.print(r.nextDouble() + " ");
    //                if (j % 3 == 2)
    //                    System.out.println();
    //            }
    //            if (!(j % 3 == 2))
    //                System.out.println();
    //
    //            System.out.println("\nGrab the first 1000 gaussian doubles");
    //            r = new MersenneTwisterFast(SEED);
    //            for (j = 0; j < 1000; j++) {
    //                System.out.print(r.nextGaussian() + " ");
    //                if (j % 3 == 2)
    //                    System.out.println();
    //            }
    //            if (!(j % 3 == 2))
    //                System.out.println();
    //
    //        }
    //    }

    private class CPPeleven_minstd_rand {
        private static final long multiplier = 0xBC8FL;
        private static final long mask = (1L << 31) - 1;
        private AtomicLong seed;

        //        public CPPeleven_minstd_rand(long seed) {
        //            this.seed = new AtomicLong(initialScramble(seed));
        //        }

        public CPPeleven_minstd_rand() {
            this.seed = new AtomicLong(initialScramble(seedUniquifier() ^ System.nanoTime()));
        }

        private long seedUniquifier() {
            // L'Ecuyer, "Tables of Linear Congruential Generators of
            // Different Sizes and Good Lattice Structure", 1999
            for (;;) {
                long current = seedUniquifier.get();
                long next = current * 48271L; //same as  0xBC8FL. In the actual java Random code the value 181783497276652981L is a typo
                // base on the paper above and should be 1181783497276652981L.
                if (seedUniquifier.compareAndSet(current, next))
                    return next;
            }
        }

        private final AtomicLong seedUniquifier = new AtomicLong(8682522807148012L);

        private long initialScramble(long seed) {
            return (seed ^ multiplier) & mask;
        }

        public int nextInt() {
            return next(32);
        }

        public int nextInt(int bound) {
            if (bound <= 0)
                throw new IllegalArgumentException("outOfBound");

            int r = next(31);
            int m = bound - 1;
            if ((bound & m) == 0) // i.e., bound is a power of 2
                r = (int) ((bound * (long) r) >> 31);
            else {
                for (int u = r; u - (r = u % bound) + m < 0; u = next(31));
            }
            return r;
        }

        public int next(int bits) {
            long oldseed, nextseed;
            AtomicLong seed = this.seed;
            do {
                oldseed = seed.get();
                nextseed = (oldseed * multiplier) & mask;
            } while (!seed.compareAndSet(oldseed, nextseed));
            return (int) (nextseed >>> (31 - bits));
        }

    }

    //    /*
    //     * https://www.iro.umontreal.ca/~lecuyer/myftp/papers/latrules99Errata.pdf
    //     * https://en.wikipedia.org/wiki/Linear_congruential_generator#cite_note-LEcuyer99-8
    //     * https://arxiv.org/pdf/1811.04035.pdf
    //     * https://www.pcg-random.org/pdf/toms-oneill-pcg-family-v1.02.pdf
    //     * */
    //    private class Best_Param_LCG_rand {
    //        private static final long multiplier = 0xE9C165L;
    //        private static final long mask = (1L << 35);
    //        private AtomicLong seed;
    //
    //        public Best_Param_LCG_rand(long seed) {
    //            this.seed = new AtomicLong(initialScramble(seed));
    //        }
    //
    //        public Best_Param_LCG_rand() {
    //            this.seed = new AtomicLong(initialScramble(seedUniquifier() ^ System.nanoTime()));
    //        }
    //
    //        private long seedUniquifier() {
    //            // L'Ecuyer, "Tables of Linear Congruential Generators of
    //            // Different Sizes and Good Lattice Structure", 1999
    //            for (;;) {
    //                long current = seedUniquifier.get();
    //                long next = current * 15319397L;
    //                if (seedUniquifier.compareAndSet(current, next))
    //                    return next;
    //            }
    //        }
    //
    //        private final AtomicLong seedUniquifier = new AtomicLong(8682522807148012L);
    //
    //        private long initialScramble(long seed) {
    //            return (seed ^ multiplier) & mask;
    //        }
    //
    //        public int nextInt() {
    //            return next(32);
    //        }
    //
    //        public int nextInt(int bound) {
    //            if (bound <= 0)
    //                throw new IllegalArgumentException("outOfBound");
    //
    //            int r = next(31);
    //            int m = bound - 1;
    //            if ((bound & m) == 0) // i.e., bound is a power of 2
    //                r = (int) ((bound * (long) r) >> 31);
    //            else {
    //                for (int u = r; u - (r = u % bound) + m < 0; u = next(31));
    //            }
    //            return r;
    //        }
    //
    //        public int next(int bits) {
    //            long oldseed, nextseed;
    //            AtomicLong seed = this.seed;
    //            do {
    //                oldseed = seed.get();
    //                nextseed = (oldseed * multiplier) & mask;
    //            } while (!seed.compareAndSet(oldseed, nextseed));
    //            return (int) (nextseed >>> (35 - bits));
    //        }
    //
    //    }

    //    private class XorShift64 extends Random {
    //        private long x;
    //
    //        public XorShift64() {
    //            x = System.nanoTime();
    //        }
    //
    //        public XorShift64(long seed) {
    //            this.x = seed;
    //        }
    //
    //        public long xorshift() {
    //            x ^= (x << 21);
    //            x ^= (x >>> 35);
    //            x ^= (x << 4);
    //            return x;
    //        }
    //
    //        public int nextInt(int bound) {
    //            if (bound <= 0)
    //                throw new IllegalArgumentException("outOfBound");
    //
    //            int r = next(31);
    //            int m = bound - 1;
    //            if ((bound & m) == 0) // i.e., bound is a power of 2
    //                r = (int) ((bound * (long) r) >> 31);
    //            else {
    //                for (int u = r; u - (r = u % bound) + m < 0; u = next(31));
    //            }
    //            return r;
    //        }
    //
    //        public int nextInt() {
    //            return next(32);
    //        }
    //
    //        protected int next(int bits) {
    //            return (int) (xorshift() >>> (Long.SIZE - bits));
    //        }
    //
    //        @Override
    //        public void setSeed(long seed) {
    //            x = seed;
    //        }
    //    }

}
