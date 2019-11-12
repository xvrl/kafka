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
package org.apache.kafka.jmh.record;

import org.apache.kafka.common.record.AbstractRecords;
import org.apache.kafka.common.record.BufferSupplier;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.TimestampType;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
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

import static org.apache.kafka.common.record.RecordBatch.CURRENT_MAGIC_VALUE;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 1)
@Measurement(iterations = 3)
public class MemoryRecordsBenchmark {

    private final Random random = new Random(0);

    public enum Bytes {
        RANDOM, ONES
    }

    @Param(value = {"1000"})
    private int batchSize = 200;

    @Param(value = {"LZ4", "SNAPPY", "ZSTD", "NONE"})
    private CompressionType compressionType = CompressionType.NONE;

    @Param(value = {"2"})
    private byte messageVersion = CURRENT_MAGIC_VALUE;

    @Param(value = { "1000", })
    private int maxMessageSize = 1000;

    @Param(value = {"RANDOM", "ONES"})
    private Bytes bytes = Bytes.RANDOM;

    // zero starting offset is much faster for v1 batches, but that will almost never happen
    private final int startingOffset = 42;

    private ByteBuffer[] messageBuffers;
    private BufferSupplier bufferSupplier;
    private ByteBuffer buf;

    @Setup
    public void init() {
        bufferSupplier = BufferSupplier.create();

        buf = ByteBuffer.allocate(
            AbstractRecords.estimateSizeInBytesUpperBound(
                messageVersion, compressionType, new byte[0], new byte[maxMessageSize],
                Record.EMPTY_HEADERS
            ) * batchSize
        );

        messageBuffers = new ByteBuffer[batchSize];
        for (int i = 0; i < batchSize; ++i) {
            int size = random.nextInt(maxMessageSize) + 1;
            messageBuffers[i] = createMessage(size);
        }
    }

    private ByteBuffer createMessage(int messageSize) {
        byte[] value = new byte[messageSize];

        switch (bytes) {
            case ONES:
                Arrays.fill(value, (byte) 1);
                break;
            case RANDOM:
                random.nextBytes(value);
                break;
        }

        return ByteBuffer.wrap(value);
    }

    @Fork(jvmArgsAppend = "-Xmx8g")
    @Benchmark
    public void measureVariableMessageSizeBatch(Blackhole bh) throws IOException {
        MemoryRecordsBuilder builder = MemoryRecords
            .builder(buf, bufferSupplier, messageVersion,
                     compressionType, TimestampType.CREATE_TIME, startingOffset);
        for (ByteBuffer b : messageBuffers) {
            bh.consume(builder.append(0, null, b));
        }
    }
}
