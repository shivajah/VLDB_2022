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

import java.util.BitSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.function.IntUnaryOperator;

/**
 * This policy is used to decide which partition in {@link VPartitionTupleBufferManager} should be a victim when
 * there is not enough space to insert new element.
 */
public class PreferToSpillFullyOccupiedFramePolicy {

    VICTIM victim;

    public enum VICTIM {
        LARGEST_RECORD,
        LARGEST_SIZE,
        SMALLEST_RECORD,
        SMALLEST_SIZE,
        MEDIAN_RECORD,
        MEDIAN_SIZE,
        MAX_SIZE_MIN_RECORD,
        RANDOM,
        HALF_EMPTY,
        LARGEST_RECORD_SELF,
        LARGEST_SIZE_SELF,
        SMALLEST_RECORD_SELF,
        SMALLEST_SIZE_SELF,
        MEDIAN_RECORD_SELF,
        MEDIAN_SIZE_SELF,
        MAX_SIZE_MIN_RECORD_SELF,
        RANDOM_SELF
    }

    private final IPartitionedTupleBufferManager bufferManager;
    private final BitSet spilledStatus;

    public PreferToSpillFullyOccupiedFramePolicy(IPartitionedTupleBufferManager bufferManager, BitSet spilledStatus,
            PreferToSpillFullyOccupiedFramePolicy.VICTIM victim) {
        this.bufferManager = bufferManager;
        this.spilledStatus = spilledStatus;
        this.victim = victim;
    }

    /**
     * This method tries to find a victim partition.
     * We want to keep in-memory partitions (not spilled to the disk yet) as long as possible to reduce the overhead
     * of writing to and reading from the disk.
     * If the given partition contains one or more tuple, then try to spill the given partition.
     * If not, try to flush another an in-memory partition.
     * Note: right now, the createAtMostOneFrameForSpilledPartitionConstrain we are using for a spilled partition
     * enforces that the number of maximum frame for a spilled partition is 1.
     */
    public int selectVictimPartition(int failedToInsertPartition, long remaining) {
        // To avoid flushing another partition with the last half-full frame, it's better to spill the given partition
        // since one partition needs to be spilled to the disk anyway. Another reason is that we know that
        // the last frame in this partition is full.
        if (spilledStatus.get(failedToInsertPartition) && bufferManager.getNumTuples(failedToInsertPartition) > 0) {
            return failedToInsertPartition;
        }
        // If the given partition doesn't contain any tuple in memory, try to flush a different in-memory partition.
        // We are not trying to steal a frame from another spilled partition since once spilled, a partition can only
        // have only one frame and we don't know whether the frame is fully occupied or not.
        // TODO: Once we change this policy (spilled partition can have only one frame in memory),
        //       we need to revise this method, too.
        if (remaining > 0) {
            return findBestMatchInMemory(remaining);
        }
        switch (victim) {
            case HALF_EMPTY:
                return findInMemPartitionWithHalfEmptyAlgorithm();
            case LARGEST_SIZE_SELF: {
                if (bufferManager.getNumTuples(failedToInsertPartition) > 0) {
                    return failedToInsertPartition;
                }
                return findInMemPartitionWithMaxMemoryUsage();
            }
            case LARGEST_RECORD_SELF: {
                if (bufferManager.getNumTuples(failedToInsertPartition) > 0) {
                    return failedToInsertPartition;
                }
                return findInMemPartitionWithMaxNumberOfRecords();
            }
            case SMALLEST_SIZE_SELF: {
                if (bufferManager.getNumTuples(failedToInsertPartition) > 0) {
                    return failedToInsertPartition;
                }
                return findInMemPartitionWithMinMemoryUsage();
            }
            case SMALLEST_RECORD_SELF: {
                if (bufferManager.getNumTuples(failedToInsertPartition) > 0) {
                    return failedToInsertPartition;
                }
                return findInMemPartitionWithMinNumberOfRecords();

            }
            case MEDIAN_SIZE_SELF: {
                if (bufferManager.getNumTuples(failedToInsertPartition) > 0) {
                    return failedToInsertPartition;
                }
                return findInMemPartitionWithMedianSize();
            }
            case MEDIAN_RECORD_SELF: {
                if (bufferManager.getNumTuples(failedToInsertPartition) > 0) {
                    return failedToInsertPartition;
                }
                return findInMemPartitionWithMedianRecord();
            }
            case RANDOM_SELF: {
                if (bufferManager.getNumTuples(failedToInsertPartition) > 0) {
                    return failedToInsertPartition;
                }
                return findInMemPartitionRandom();
            }
            case MAX_SIZE_MIN_RECORD_SELF: {
                if (bufferManager.getNumTuples(failedToInsertPartition) > 0) {
                    return failedToInsertPartition;
                }
                return findInMemPartitionWithMaxSizeToMinRecord();
            }
            //            case MIN_SIZE_MAX_RECORD_SELF: {
            //                if (bufferManager.getNumTuples(failedToInsertPartition) > 0) {
            //                    return failedToInsertPartition;
            //                }
            //                return findInMemPartitionWithMinSizeToMaxRecord();
            //            }
            case LARGEST_SIZE:
                if (spilledStatus.get(failedToInsertPartition)
                        && bufferManager.getNumTuples(failedToInsertPartition) > 0) {
                    return failedToInsertPartition;
                }
                return findInMemPartitionWithMaxMemoryUsage();
            case LARGEST_RECORD:
                if (spilledStatus.get(failedToInsertPartition)
                        && bufferManager.getNumTuples(failedToInsertPartition) > 0) {
                    return failedToInsertPartition;
                }
                return findInMemPartitionWithMaxNumberOfRecords();
            case SMALLEST_SIZE:
                if (spilledStatus.get(failedToInsertPartition)
                        && bufferManager.getNumTuples(failedToInsertPartition) > 0) {
                    return failedToInsertPartition;
                }
                return findInMemPartitionWithMinMemoryUsage();
            case SMALLEST_RECORD:
                if (spilledStatus.get(failedToInsertPartition)
                        && bufferManager.getNumTuples(failedToInsertPartition) > 0) {
                    return failedToInsertPartition;
                }
                return findInMemPartitionWithMinNumberOfRecords();
            case MEDIAN_SIZE:
                if (spilledStatus.get(failedToInsertPartition)
                        && bufferManager.getNumTuples(failedToInsertPartition) > 0) {
                    return failedToInsertPartition;
                }
                return findInMemPartitionWithMedianSize();
            case MEDIAN_RECORD:
                if (spilledStatus.get(failedToInsertPartition)
                        && bufferManager.getNumTuples(failedToInsertPartition) > 0) {
                    return failedToInsertPartition;
                }
                return findInMemPartitionWithMedianRecord();
            case RANDOM:
                if (spilledStatus.get(failedToInsertPartition)
                        && bufferManager.getNumTuples(failedToInsertPartition) > 0) {
                    return failedToInsertPartition;
                }
                return findInMemPartitionRandom();
            case MAX_SIZE_MIN_RECORD:
                if (spilledStatus.get(failedToInsertPartition)
                        && bufferManager.getNumTuples(failedToInsertPartition) > 0) {
                    return failedToInsertPartition;
                }
                return findInMemPartitionWithMaxSizeToMinRecord();
            //            case MIN_SIZE_MAX_RECORD:
            //                if (spilledStatus.get(failedToInsertPartition)
            //                        && bufferManager.getNumTuples(failedToInsertPartition) > 0) {
            //                    return failedToInsertPartition;
            //                }
            //                return findInMemPartitionWithMinSizeToMaxRecord();

            default:
                if (spilledStatus.get(failedToInsertPartition)
                        && bufferManager.getNumTuples(failedToInsertPartition) > 0) {
                    return failedToInsertPartition;
                }
                return findInMemPartitionWithMaxMemoryUsage();

        }
    }

    public int selectSpilledVictimPartition(int failedToInsertPartition, int frameSize) {
        if (bufferManager.getNumTuples(failedToInsertPartition) > 0
                && bufferManager.getPhysicalSize(failedToInsertPartition) > frameSize
                && spilledStatus.get(failedToInsertPartition)) {
            return failedToInsertPartition;
        }

        int biggestSpilled = findSpilledPartitionWithMaxMemoryUsage();
        if (biggestSpilled >= 0 && bufferManager.getPhysicalSize(biggestSpilled) > frameSize) {
            return biggestSpilled;
        }
        return -1;
    }

    public int findInMemPartitionWithMinMemoryUsage() {//Size based
        return findMinSize(spilledStatus.nextClearBit(0), (i) -> spilledStatus.nextClearBit(i + 1));
    }

    public int findInMemPartitionWithMaxMemoryUsage() {
        return findMaxSize(spilledStatus.nextClearBit(0), (i) -> spilledStatus.nextClearBit(i + 1));
    }

    public int findSpilledPartitionWithMaxMemoryUsage() {
        return findMaxSize(spilledStatus.nextSetBit(0), (i) -> spilledStatus.nextSetBit(i + 1));
    }

    public int findInMemPartitionWithMaxNumberOfRecords() {
        return findMaxRecord(spilledStatus.nextClearBit(0), i -> spilledStatus.nextClearBit(i + 1));
    }

    public int findInMemPartitionWithMinNumberOfRecords() {
        return findMinRecord(spilledStatus.nextClearBit(0), i -> spilledStatus.nextClearBit(i + 1));
    }

    public int findInMemPartitionWithMedianSize() {
        return findMedianSize(spilledStatus.nextClearBit(0), i -> spilledStatus.nextClearBit(i + 1));
    }

    public int findInMemPartitionWithMedianRecord() {
        return findMedianRecord(spilledStatus.nextClearBit(0), i -> spilledStatus.nextClearBit(i + 1));
    }

    public int findInMemPartitionWithMaxSizeToMinRecord() {
        return findMaxSizeToMinRecord(spilledStatus.nextClearBit(0), i -> spilledStatus.nextClearBit(i + 1));
    }

    public int findInMemPartitionWithHalfEmptyAlgorithm() {
        if (spilledStatus.cardinality() <= spilledStatus.length() / 2) {
            return findInMemPartitionWithMinMemoryUsage();
        }
        return findInMemPartitionWithMaxMemoryUsage();
    }

    public int findInMemPartitionWithMinSizeToMaxRecord() {
        return findMinSizeToMaxRecord(spilledStatus.nextClearBit(0), i -> spilledStatus.nextClearBit(i + 1));
    }
    //Median

    public int findInMemPartitionRandom() {
        Random random = new Random();
        int randNum = random.nextInt(bufferManager.getNumPartitions());
        return findRand(randNum, i -> spilledStatus.nextClearBit(i + 1));
    }

    private int findBestMatchInMemory(long remaining) {
        int pid = -1;
        long diff = Long.MAX_VALUE;
        for (int i = spilledStatus.nextClearBit(0); i >= 0 && i < bufferManager.getNumPartitions(); i =
                spilledStatus.nextClearBit(i + 1)) {
            int pSize = bufferManager.getPhysicalSize(i);
            long pDiff = pSize - remaining;
            if (pSize >= remaining && pDiff < diff) {
                pid = i;
                diff = pDiff;
            }
        }
        if (pid >= 0) {
            return pid;
        }
        return findInMemPartitionWithMinMemoryUsage();
    }

    private int findMaxSizeToMinRecord(int startIndex, IntUnaryOperator nextIndexOp) {
        double max = -1;
        int pid = -1;
        for (int i = startIndex; i >= 0 && i < bufferManager.getNumPartitions(); i = nextIndexOp.applyAsInt(i)) {
            int numberOfRecords = bufferManager.getNumTuples(i);
            int size = bufferManager.getPhysicalSize(i);
            if (numberOfRecords == 0)
                continue;
            double ratio = (size * 1.0) / (numberOfRecords * 1.0);
            if (ratio > max) {
                max = ratio;
                pid = i;
            }
        }
        return pid;
    }

    private int findMinSizeToMaxRecord(int startIndex, IntUnaryOperator nextIndexOp) {
        double min = Double.MIN_VALUE;
        int pid = -1;
        for (int i = startIndex; i >= 0 && i < bufferManager.getNumPartitions(); i = nextIndexOp.applyAsInt(i)) {
            int numberOfRecords = bufferManager.getNumTuples(i);
            int size = bufferManager.getPhysicalSize(i);
            if (numberOfRecords == 0)
                continue;
            double ratio = (size * 1.0) / (numberOfRecords * 1.0);
            if (ratio < min) {
                min = ratio;
                pid = i;
            }
        }
        return pid;
    }

    private int findRand(int startIndex, IntUnaryOperator nextIndexOp) {

        int numberOfInMemoryPartitions = bufferManager.getNumPartitions() - spilledStatus.cardinality();
        int checked = 0;
        int i = nextIndexOp.applyAsInt(startIndex);
        while (checked < numberOfInMemoryPartitions) {
            if (bufferManager.getNumTuples(i) > 1) {
                return i;
            }
            i = nextIndexOp.applyAsInt(i);
            checked++;
        }
        return -1;
    }

    private int findMedianSize(int startIndex, IntUnaryOperator nextIndexOp) {
        TreeMap<Integer, List<Integer>> sizeToPid = new TreeMap<>();
        int medCounter = 0;//number of in memory partitions
        for (int i = startIndex; i >= 0 && i < bufferManager.getNumPartitions(); i = nextIndexOp.applyAsInt(i)) {
            medCounter++;
            int size = bufferManager.getPhysicalSize(i);
            if (!sizeToPid.containsKey(size)) {
                List<Integer> vals = new LinkedList<>();
                sizeToPid.put(size, vals);
            }
            sizeToPid.get(size).add(i);
        }
        int counter = 0;
        for (Map.Entry<Integer, List<Integer>> entry : sizeToPid.entrySet()) {
            List<Integer> vals = entry.getValue();
            for (Integer v : vals) {
                if (counter == medCounter)
                    return v;
                else
                    counter++;
            }
        }
        return -1;
    }

    private int findMedianRecord(int startIndex, IntUnaryOperator nextIndexOp) {
        TreeMap<Integer, List<Integer>> sizeToPid = new TreeMap<>();
        int medCounter = 0;//number of in memory partitions
        for (int i = startIndex; i >= 0 && i < bufferManager.getNumPartitions(); i = nextIndexOp.applyAsInt(i)) {
            medCounter++;
            int size = bufferManager.getNumTuples(i);
            if (!sizeToPid.containsKey(size)) {
                List<Integer> vals = new LinkedList<>();
                sizeToPid.put(size, vals);
            }
            sizeToPid.get(size).add(i);
        }
        int counter = 0;
        for (Map.Entry<Integer, List<Integer>> entry : sizeToPid.entrySet()) {
            List<Integer> vals = entry.getValue();
            for (Integer v : vals) {
                if (counter == medCounter)
                    return v;
                else
                    counter++;
            }
        }
        return -1;
    }

    private int findMaxRecord(int startIndex, IntUnaryOperator nextIndexOp) {
        int pid = -1;
        int max = -1;
        for (int i = startIndex; i >= 0 && i < bufferManager.getNumPartitions(); i = nextIndexOp.applyAsInt(i)) {
            //This is the total number of tuples (in memory and in disk), but it works for us cause we are calling
            // this method only on in memory partitions, so it will returns the number of tuples in memory for that
            // partition.
            int numberOfRecords = bufferManager.getNumTuples(i);
            if (numberOfRecords > max) {
                max = numberOfRecords;
                pid = i;
            }
        }
        return pid;
    }

    private int findMinRecord(int startIndex, IntUnaryOperator nextIndexOp) {
        int pid = -1;
        int min = Integer.MAX_VALUE;
        for (int i = startIndex; i >= 0 && i < bufferManager.getNumPartitions(); i = nextIndexOp.applyAsInt(i)) {
            int numberOfRecords = bufferManager.getNumTuples(i);
            if (numberOfRecords > 0 && numberOfRecords < min) {
                min = numberOfRecords;
                pid = i;
            }
        }
        return pid;
    }

    private int findMinSize(int startIndex, IntUnaryOperator nextIndexOp) {
        int pid = -1;
        int min = Integer.MAX_VALUE;
        for (int i = startIndex; i >= 0 && i < bufferManager.getNumPartitions(); i = nextIndexOp.applyAsInt(i)) {
            int partSize = bufferManager.getPhysicalSize(i);
            if (partSize > 0 && partSize < min) {
                min = partSize;
                pid = i;
            }
        }
        return pid;
    }

    private int findMaxSize(int startIndex, IntUnaryOperator nextIndexOp) {
        int pid = -1;
        int max = 0;
        for (int i = startIndex; i >= 0 && i < bufferManager.getNumPartitions(); i = nextIndexOp.applyAsInt(i)) {
            int partSize = bufferManager.getPhysicalSize(i);
            if (partSize > max) {
                max = partSize;
                pid = i;
            }
        }
        return pid;
    }

    /**
     * Create a constrain for the already spilled partition that it can only use at most one frame.
     *
     * @param spillStatus
     * @return
     */
    public static IPartitionedMemoryConstrain createAtMostOneFrameForSpilledPartitionConstrain(BitSet spillStatus) {
        return new IPartitionedMemoryConstrain() {
            @Override
            public int frameLimit(int partitionId) {
                if (spillStatus.get(partitionId)) {
                    return 1;
                }
                return Integer.MAX_VALUE;
            }
        };
    }
}
