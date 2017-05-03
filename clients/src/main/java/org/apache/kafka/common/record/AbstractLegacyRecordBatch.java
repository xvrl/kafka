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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.utils.AbstractIterator;
import org.apache.kafka.common.utils.ByteBufferInputStream;
import org.apache.kafka.common.utils.ByteUtils;
import org.apache.kafka.common.utils.CloseableIterator;
import org.apache.kafka.common.utils.Utils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.NoSuchElementException;

import static org.apache.kafka.common.record.Records.LOG_OVERHEAD;
import static org.apache.kafka.common.record.Records.OFFSET_OFFSET;

/**
 * This {@link RecordBatch} implementation is for magic versions 0 and 1. In addition to implementing
 * {@link RecordBatch}, it also implements {@link Record}, which exposes the duality of the old message
 * format in its handling of compressed messages. The wrapper record is considered the record batch in this
 * interface, while the inner records are considered the log records (though they both share the same schema).
 *
 * In general, this class should not be used directly. Instances of {@link Records} provides access to this
 * class indirectly through the {@link RecordBatch} interface.
 */
public abstract class AbstractLegacyRecordBatch extends AbstractRecordBatch implements Record {

    public abstract LegacyRecord outerRecord();

    @Override
    public long lastOffset() {
        return offset();
    }

    @Override
    public boolean isValid() {
        return outerRecord().isValid();
    }

    @Override
    public void ensureValid() {
        outerRecord().ensureValid();
    }

    @Override
    public int keySize() {
        return outerRecord().keySize();
    }

    @Override
    public boolean hasKey() {
        return outerRecord().hasKey();
    }

    @Override
    public ByteBuffer key() {
        return outerRecord().key();
    }

    @Override
    public int valueSize() {
        return outerRecord().valueSize();
    }

    @Override
    public boolean hasValue() {
        return !outerRecord().hasNullValue();
    }

    @Override
    public ByteBuffer value() {
        return outerRecord().value();
    }

    @Override
    public Header[] headers() {
        return Record.EMPTY_HEADERS;
    }

    @Override
    public boolean hasMagic(byte magic) {
        return magic == outerRecord().magic();
    }

    @Override
    public boolean hasTimestampType(TimestampType timestampType) {
        return outerRecord().timestampType() == timestampType;
    }

    @Override
    public long checksum() {
        return outerRecord().checksum();
    }

    @Override
    public long maxTimestamp() {
        return timestamp();
    }

    @Override
    public long timestamp() {
        return outerRecord().timestamp();
    }

    @Override
    public TimestampType timestampType() {
        return outerRecord().timestampType();
    }

    @Override
    public long baseOffset() {
        return iterator().next().offset();
    }

    @Override
    public byte magic() {
        return outerRecord().magic();
    }

    @Override
    public CompressionType compressionType() {
        return outerRecord().compressionType();
    }

    @Override
    public int sizeInBytes() {
        return outerRecord().sizeInBytes() + LOG_OVERHEAD;
    }

    @Override
    public Integer countOrNull() {
        return null;
    }

    @Override
    public String toString() {
        return "LegacyRecordBatch(offset=" + offset() + ", " + outerRecord() + ")";
    }

    @Override
    public void writeTo(ByteBuffer buffer) {
        writeHeader(buffer, offset(), outerRecord().sizeInBytes());
        buffer.put(outerRecord().buffer().duplicate());
    }

    @Override
    public long producerId() {
        return RecordBatch.NO_PRODUCER_ID;
    }

    @Override
    public short producerEpoch() {
        return RecordBatch.NO_PRODUCER_EPOCH;
    }

    @Override
    public boolean hasProducerId() {
        return false;
    }

    @Override
    public long sequence() {
        return RecordBatch.NO_SEQUENCE;
    }

    @Override
    public int baseSequence() {
        return RecordBatch.NO_SEQUENCE;
    }

    @Override
    public int lastSequence() {
        return RecordBatch.NO_SEQUENCE;
    }

    @Override
    public boolean isTransactional() {
        return false;
    }

    @Override
    public int partitionLeaderEpoch() {
        return RecordBatch.NO_PARTITION_LEADER_EPOCH;
    }

    @Override
    public boolean isControlRecord() {
        return false;
    }

    /**
     * Get an iterator for the nested entries contained within this batch. Note that
     * if the batch is not compressed, then this method will return an iterator over the
     * shallow record only (i.e. this object).
     * @return An iterator over the records contained within this batch
     */
    @Override
    public CloseableIterator<Record> iterator() {
        if (isCompressed())
            return new DeepRecordsIterator(this, false, Integer.MAX_VALUE);

        return new CloseableIterator<Record>() {
            private boolean hasNext = true;

            @Override
            public void close() {}

            @Override
            public boolean hasNext() {
                return hasNext;
            }

            @Override
            public Record next() {
                if (!hasNext)
                    throw new NoSuchElementException();
                hasNext = false;
                return AbstractLegacyRecordBatch.this;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    public CloseableIterator<Record> streamingIterator() {
        // the older message format versions do not support streaming, so we return the normal iterator
        return iterator();
    }

    static void writeHeader(ByteBuffer buffer, long offset, int size) {
        buffer.putLong(offset);
        buffer.putInt(size);
    }

    static void writeHeader(DataOutputStream out, long offset, int size) throws IOException {
        out.writeLong(offset);
        out.writeInt(size);
    }

    private static final class DataLogInputStream implements LogInputStream<AbstractLegacyRecordBatch> {
        private final DataInputStream stream;
        protected final int maxMessageSize;

        DataLogInputStream(DataInputStream stream, int maxMessageSize) {
            this.stream = stream;
            this.maxMessageSize = maxMessageSize;
        }

        public AbstractLegacyRecordBatch nextBatch() throws IOException {
            try {
                long offset = stream.readLong();
                int size = stream.readInt();
                if (size < LegacyRecord.RECORD_OVERHEAD_V0)
                    throw new CorruptRecordException(String.format("Record size is less than the minimum record overhead (%d)", LegacyRecord.RECORD_OVERHEAD_V0));
                if (size > maxMessageSize)
                    throw new CorruptRecordException(String.format("Record size exceeds the largest allowable message size (%d).", maxMessageSize));

                byte[] recordBuffer = new byte[size];
                stream.readFully(recordBuffer, 0, size);
                ByteBuffer buf = ByteBuffer.wrap(recordBuffer);
                return new BasicLegacyRecordBatch(offset, new LegacyRecord(buf));
            } catch (EOFException e) {
                return null;
            }
        }
    }

    private static class DeepRecordsIterator extends AbstractIterator<Record> implements CloseableIterator<Record> {
        private final ArrayDeque<AbstractLegacyRecordBatch> batches;
        private final long absoluteBaseOffset;
        private final byte wrapperMagic;

        private DeepRecordsIterator(AbstractLegacyRecordBatch wrapperEntry, boolean ensureMatchingMagic, int maxMessageSize) {
            LegacyRecord wrapperRecord = wrapperEntry.outerRecord();
            this.wrapperMagic = wrapperRecord.magic();

            CompressionType compressionType = wrapperRecord.compressionType();
            ByteBuffer wrapperValue = wrapperRecord.value();
            if (wrapperValue == null)
                throw new InvalidRecordException("Found invalid compressed record set with null value (magic = " +
                        wrapperMagic + ")");

            DataInputStream stream = new DataInputStream(compressionType.wrapForInput(wrapperValue, wrapperRecord.magic()));
            LogInputStream<AbstractLegacyRecordBatch> logStream = new DataLogInputStream(stream, maxMessageSize);

            long wrapperRecordOffset = wrapperEntry.lastOffset();
            long wrapperRecordTimestamp = wrapperRecord.timestamp();
            this.batches = new ArrayDeque<>();

            // If relative offset is used, we need to decompress the entire message first to compute
            // the absolute offset. For simplicity and because it's a format that is on its way out, we
            // do the same for message format version 0
            try {
                while (true) {
                    AbstractLegacyRecordBatch batch = logStream.nextBatch();
                    if (batch == null)
                        break;

                    LegacyRecord record = batch.outerRecord();
                    byte magic = record.magic();

                    if (ensureMatchingMagic && magic != wrapperMagic)
                        throw new InvalidRecordException("Compressed message magic " + magic +
                                " does not match wrapper magic " + wrapperMagic);

                    if (magic > RecordBatch.MAGIC_VALUE_V0) {
                        LegacyRecord recordWithTimestamp = new LegacyRecord(
                                record.buffer(),
                                wrapperRecordTimestamp,
                                wrapperRecord.timestampType());
                        batch = new BasicLegacyRecordBatch(batch.lastOffset(), recordWithTimestamp);
                    }
                    batches.addLast(batch);

                    // break early if we reach the last offset in the batch
                    if (batch.offset() == wrapperRecordOffset)
                        break;
                }

                if (batches.isEmpty())
                    throw new InvalidRecordException("Found invalid compressed record set with no inner records");

                if (wrapperMagic > RecordBatch.MAGIC_VALUE_V0)
                    this.absoluteBaseOffset = wrapperRecordOffset - batches.getLast().lastOffset();
                else
                    this.absoluteBaseOffset = -1;
            } catch (IOException e) {
                throw new KafkaException(e);
            } finally {
                Utils.closeQuietly(stream, "records iterator stream");
            }
        }

        @Override
        protected Record makeNext() {
            if (batches.isEmpty())
                return allDone();

            AbstractLegacyRecordBatch entry = batches.remove();

            // Convert offset to absolute offset if needed.
            if (absoluteBaseOffset >= 0) {
                long absoluteOffset = absoluteBaseOffset + entry.lastOffset();
                entry = new BasicLegacyRecordBatch(absoluteOffset, entry.outerRecord());
            }

            if (entry.isCompressed())
                throw new InvalidRecordException("Inner messages must not be compressed");

            return entry;
        }

        @Override
        public void close() {}
    }

    private static class BasicLegacyRecordBatch extends AbstractLegacyRecordBatch {
        private final LegacyRecord record;
        private final long offset;

        private BasicLegacyRecordBatch(long offset, LegacyRecord record) {
            this.offset = offset;
            this.record = record;
        }

        @Override
        public long offset() {
            return offset;
        }

        @Override
        public LegacyRecord outerRecord() {
            return record;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            BasicLegacyRecordBatch that = (BasicLegacyRecordBatch) o;

            return offset == that.offset &&
                    (record != null ? record.equals(that.record) : that.record == null);
        }

        @Override
        public int hashCode() {
            int result = record != null ? record.hashCode() : 0;
            result = 31 * result + (int) (offset ^ (offset >>> 32));
            return result;
        }
    }

    static class ByteBufferLegacyRecordBatch extends AbstractLegacyRecordBatch implements MutableRecordBatch {
        private final ByteBuffer buffer;
        private final LegacyRecord record;

        ByteBufferLegacyRecordBatch(ByteBuffer buffer) {
            this.buffer = buffer;
            buffer.position(LOG_OVERHEAD);
            this.record = new LegacyRecord(buffer.slice());
            buffer.position(OFFSET_OFFSET);
        }

        @Override
        public long offset() {
            return buffer.getLong(OFFSET_OFFSET);
        }

        @Override
        public LegacyRecord outerRecord() {
            return record;
        }

        @Override
        public void setLastOffset(long offset) {
            buffer.putLong(OFFSET_OFFSET, offset);
        }

        @Override
        public void setMaxTimestamp(TimestampType timestampType, long timestamp) {
            if (record.magic() == RecordBatch.MAGIC_VALUE_V0)
                throw new UnsupportedOperationException("Cannot set timestamp for a record with magic = 0");

            long currentTimestamp = record.timestamp();
            // We don't need to recompute crc if the timestamp is not updated.
            if (record.timestampType() == timestampType && currentTimestamp == timestamp)
                return;

            setTimestampAndUpdateCrc(timestampType, timestamp);
        }

        @Override
        public void setPartitionLeaderEpoch(int epoch) {
            throw new UnsupportedOperationException("Magic versions prior to 2 do not support partition leader epoch");
        }

        private void setTimestampAndUpdateCrc(TimestampType timestampType, long timestamp) {
            byte attributes = LegacyRecord.computeAttributes(magic(), compressionType(), timestampType);
            buffer.put(LOG_OVERHEAD + LegacyRecord.ATTRIBUTES_OFFSET, attributes);
            buffer.putLong(LOG_OVERHEAD + LegacyRecord.TIMESTAMP_OFFSET, timestamp);
            long crc = record.computeChecksum();
            ByteUtils.writeUnsignedInt(buffer, LOG_OVERHEAD + LegacyRecord.CRC_OFFSET, crc);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            ByteBufferLegacyRecordBatch that = (ByteBufferLegacyRecordBatch) o;

            return buffer != null ? buffer.equals(that.buffer) : that.buffer == null;
        }

        @Override
        public int hashCode() {
            return buffer != null ? buffer.hashCode() : 0;
        }
    }

}
