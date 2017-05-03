/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.record;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

import static org.apache.kafka.common.record.RecordBatch.MAGIC_VALUE_V1;

@State(Scope.Benchmark)
public class DeepRecordsIteratorBenchmark {

    final Random random = new Random(0);
    final int batchCount = 5000;

    private int maxBatchSize = 10;

    public enum Bytes {
        RANDOM, ONES
    }

    @Param(value = {"LZ4", "SNAPPY"})
    private CompressionType type = CompressionType.NONE;

    @Param(value = {"1", "2"})
    byte messageVersion = MAGIC_VALUE_V1;

    @Param(value = {"100", "1000", "10000", "100000"})
    private int messageSize = 100;

    @Param(value = {"RANDOM", "ONES"})
    private Bytes bytes = Bytes.RANDOM;

    // zero starting offset is much faster for v1 batches, but that will almost never happen
    int startingOffset = 42;

    ByteBuffer theBuffer;

    ByteBuffer[] batch;
    int[] batchSize;

    @Setup
    public void init() {
        theBuffer = createBatch(1);

        batch = new ByteBuffer[batchCount];
        batchSize = new int[batchCount];
        for (int i = 0; i < batchCount; ++i) {
            int size = random.nextInt(maxBatchSize) + 1;
            batch[i] = createBatch(size);
            this.batchSize[i] = size;
        }
    }

    private ByteBuffer createBatch(int batchSize) {
        byte[] value = new byte[messageSize];
        final ByteBuffer buf = ByteBuffer.allocate((messageSize + 8 + 4) * batchSize);

        final MemoryRecordsBuilder builder =
            MemoryRecords.builder(buf, messageVersion, type, TimestampType.CREATE_TIME, startingOffset);

        for (int i = 0; i < batchSize; ++i) {
            switch (bytes) {
                case ONES:
                    Arrays.fill(value, (byte) 1);
                    break;
                case RANDOM:
                    random.nextBytes(value);
                    break;
            }

            builder.append(0, null, value);
        }
        return builder.build().buffer();
    }

    @Fork(value = 1)
    @Warmup(iterations = 5)
    @Benchmark
    public void measureSingleMessage(Blackhole bh) throws IOException {
        for (Record record : new ByteBufferLogInputStream(theBuffer.duplicate(), Integer.MAX_VALUE).nextBatch()) {
            bh.consume(record);
        }
    }

    @Fork(value = 1)
    @Warmup(iterations = 5)
    @OperationsPerInvocation(value = batchCount)
    @Benchmark
    public void measureVariableBatchSize(Blackhole bh) throws IOException {
        for (int i = 0; i < batchCount; ++i) {
            for (Record record : new ByteBufferLogInputStream(batch[i].duplicate(), Integer.MAX_VALUE).nextBatch()) {
                bh.consume(record);
            }
        }
    }

}
