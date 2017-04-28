package org.apache.kafka.common.record;

import org.apache.kafka.common.record.RecordsIterator.DeepRecordsIterator;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

@State(Scope.Benchmark)
public class DeepRecordsIteratorBenchmark {

  final Random RANDOM = new Random(0);
  final int BATCH_COUNT = 5000;

  private int maxBatchSize = 10;

  @Param(value = {"LZ4", "SNAPPY", "NONE"})
  private CompressionType compressionType = CompressionType.LZ4;

  @Param(value = {"100", "1000", "10000", "100000"})
  private int messageSize = 100;

  byte[] theData;

  byte[][] batch;
  int[] batchSize;

  @Setup
  public void init() {
    theData = createBatch(1);

    batch = new byte[BATCH_COUNT][];
    batchSize = new int[BATCH_COUNT];
    for(int i = 0; i < BATCH_COUNT; ++i) {
      int size = RANDOM.nextInt(maxBatchSize) + 1;
      batch[i] = createBatch(size);
      this.batchSize[i] = size;
    }
  }

  private byte[] createBatch(int batchSize) {
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

    ByteBuffer theBuffer = ByteBuffer.allocate(recordSize * batchSize + 1024);

    final DataOutputStream dataOutputStream = MemoryRecordsBuilder.wrapForOutput(
        new ByteBufferOutputStream(theBuffer), compressionType,
        Record.MAGIC_VALUE_V1, 1 << 13
    );

    try {
      dataOutputStream.write(data);
      dataOutputStream.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    theBuffer.flip();
    byte[] res = new byte[theBuffer.limit()];
    ByteBuffer.wrap(res).put(theBuffer);

    return res;
  }

  @Fork(value = 1)
  @Warmup(iterations = 5)
  @org.openjdk.jmh.annotations.Benchmark
  public DeepRecordsIterator measureSingleMessage() {
    return new DeepRecordsIterator(LogEntry.create(0, Record.create(
        Record.MAGIC_VALUE_V1,
        1,
        new byte[]{},
        theData,
        compressionType,
        TimestampType.NO_TIMESTAMP_TYPE
    )), false, Integer.MAX_VALUE);
  }

  @Fork(value = 1)
  @Warmup(iterations = 5)
  @OperationsPerInvocation(value = BATCH_COUNT)
  @Benchmark
  public void measureVariableBatchSize(Blackhole bh) {
    for (int i = 0; i < BATCH_COUNT; ++i) {
      bh.consume(new DeepRecordsIterator(LogEntry.create(0, Record.create(
          Record.MAGIC_VALUE_V1,
          batchSize[i],
          new byte[]{},
          batch[i],
          compressionType,
          TimestampType.NO_TIMESTAMP_TYPE
      )), false, Integer.MAX_VALUE));
    }
  }

  public static void main(String[] args) {
    final DeepRecordsIteratorBenchmark
        benchmark =
        new DeepRecordsIteratorBenchmark();

    benchmark.init();
    benchmark.measureSingleMessage();
  }
}
