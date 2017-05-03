package org.apache.kafka.common.record;

import org.apache.kafka.common.record.AbstractLegacyRecordBatch.ByteBufferLegacyRecordBatch;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
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
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Random;

import static org.apache.kafka.common.record.RecordBatch.MAGIC_VALUE_V1;

@State(Scope.Benchmark)
public class DeepRecordsIteratorBenchmark {

  final Random RANDOM = new Random(0);
  final int BATCH_COUNT = 5000;

  private int maxBatchSize = 10;

  @Param(value = {"LZ4", "SNAPPY", "NONE"})
  private CompressionType type = CompressionType.LZ4;

  @Param(value = {"100", "1000", "10000", "100000"})
  private int messageSize = 100;

  ByteBuffer theBuffer;

  ByteBuffer[] batch;
  int[] batchSize;

  @Setup
  public void init() {
    theBuffer = createBatch(1);

    batch = new ByteBuffer[BATCH_COUNT];
    batchSize = new int[BATCH_COUNT];
    for(int i = 0; i < BATCH_COUNT; ++i) {
      int size = RANDOM.nextInt(maxBatchSize) + 1;
      batch[i] = createBatch(size);
      this.batchSize[i] = size;
    }
  }

  private ByteBuffer createBatch(int batchSize) {
    byte[] rand = new byte[messageSize];
    int recordSize = messageSize + 8 + 4;
    byte[] data = new byte[recordSize * batchSize];
    final ByteBuffer buf = ByteBuffer.wrap(data);
    for (int i = 0; i < batchSize; ++i) {
      RANDOM.nextBytes(rand);
      buf.putLong(i) // offset
          .putInt(messageSize) // size
          .put(rand); // bytes
    }

    ByteBuffer out = ByteBuffer.allocate(recordSize * batchSize + 1024);

    final OutputStream dataOutputStream = type.wrapForOutput(
        new ByteBufferOutputStream(out), MAGIC_VALUE_V1, 1 << 13
    );

    try {
      dataOutputStream.write(data);
      dataOutputStream.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    out.flip();

    byte[] value = new byte[out.remaining()];
    ByteBuffer.wrap(value).put(out);

    LegacyRecord record = LegacyRecord.create(
        MAGIC_VALUE_V1,
        0,
        new byte[]{},
        value,
        type,
        TimestampType.CREATE_TIME
    );

    ByteBuffer buffer = ByteBuffer.allocateDirect(record.buffer().remaining() + 8 + 4);
    buffer.putLong(0) // offset
        .putInt(value.length)
        .put(record.buffer())
        .rewind();

    return buffer;
  }

  @Fork(value = 1)
  @Warmup(iterations = 5)
  @org.openjdk.jmh.annotations.Benchmark
  public AbstractLegacyRecordBatch.DeepRecordsIterator measureSingleMessage() {

    return new AbstractLegacyRecordBatch.DeepRecordsIterator(
        new ByteBufferLegacyRecordBatch(theBuffer),
        false,
        Integer.MAX_VALUE
    );
  }

  @Fork(value = 1)
  @Warmup(iterations = 5)
  @OperationsPerInvocation(value = BATCH_COUNT)
  @Benchmark
  public void measureVariableBatchSize(Blackhole bh) {
    for (int i = 0; i < BATCH_COUNT; ++i) {
      bh.consume(
          new AbstractLegacyRecordBatch.DeepRecordsIterator(
              new ByteBufferLegacyRecordBatch(batch[i]),
              false,
              Integer.MAX_VALUE
          )
      );
    }
  }
}
