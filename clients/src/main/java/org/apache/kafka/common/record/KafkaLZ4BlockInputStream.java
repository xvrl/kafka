/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import static org.apache.kafka.common.record.KafkaLZ4BlockOutputStream.LZ4_FRAME_INCOMPRESSIBLE_MASK;
import static org.apache.kafka.common.record.KafkaLZ4BlockOutputStream.MAGIC;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.kafka.clients.producer.internals.BufferPool;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.record.KafkaLZ4BlockOutputStream.BD;
import org.apache.kafka.common.record.KafkaLZ4BlockOutputStream.FLG;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;

import net.jpountz.lz4.LZ4Exception;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4SafeDecompressor;
import net.jpountz.xxhash.XXHash32;
import net.jpountz.xxhash.XXHashFactory;

/**
 * A partial implementation of the v1.5.1 LZ4 Frame format.
 *
 * @see <a href="https://github.com/lz4/lz4/wiki/lz4_Frame_format.md">LZ4 Frame Format</a>
 */
public final class KafkaLZ4BlockInputStream extends InputStream {

    public static final String PREMATURE_EOS = "Stream ended prematurely";
    public static final String NOT_SUPPORTED = "Stream unsupported (invalid magic bytes)";
    public static final String BLOCK_HASH_MISMATCH = "Block checksum mismatch";
    public static final String DESCRIPTOR_HASH_MISMATCH = "Stream frame descriptor corrupted";

    private static final LZ4SafeDecompressor DECOMPRESSOR = LZ4Factory.fastestInstance().safeDecompressor();
    private static final XXHash32 CHECKSUM = XXHashFactory.fastestInstance().hash32();
    private static final BufferPool FREE = new BufferPool(1024 * 1024, 1 << 16, new Metrics(),
                                                          Time.SYSTEM, "lz4-buffer-pool");

    private final ByteBuffer in;

    private ByteBuffer _buffer;
    private final ByteBuffer decompressionBuffer;
    private final int maxBlockSize;
    private final boolean ignoreFlagDescriptorChecksum;
    private FLG flg;
    private BD bd;
    private boolean finished;

    /**
     * Create a new {@link InputStream} that will decompress data using the LZ4 algorithm.
     *
     * @param in The stream to decompress
     * @param ignoreFlagDescriptorChecksum for compatibility with old kafka clients, ignore incorrect HC byte
     * @throws IOException
     */
    public KafkaLZ4BlockInputStream(ByteBuffer in, boolean ignoreFlagDescriptorChecksum) throws IOException {
        this.in = in.duplicate().order(ByteOrder.LITTLE_ENDIAN);

        this.ignoreFlagDescriptorChecksum = ignoreFlagDescriptorChecksum;
        readHeader();
        maxBlockSize = bd.getBlockMaximumSize();

        try {
            decompressionBuffer = FREE.allocate(maxBlockSize, 1000);
            if (!decompressionBuffer.hasArray()) {
                // workaround for https://github.com/lz4/lz4-java/pull/65
                throw new RuntimeException("decompression buffer must be array backed");
            }
        } catch(InterruptedException e) {
            throw new RuntimeException(e);
        }

        finished = false;
    }

    /**
     * Create a new {@link InputStream} that will decompress data using the LZ4 algorithm.
     *
     * @param in The stream to decompress
     * @throws IOException
     */
    public KafkaLZ4BlockInputStream(ByteBuffer in) throws IOException {
        this(in, false);
    }

    /**
     * Check whether KafkaLZ4BlockInputStream is configured to ignore the
     * Frame Descriptor checksum, which is useful for compatibility with
     * old client implementations that use incorrect checksum calculations.
     */
    public boolean ignoreFlagDescriptorChecksum() {
        return this.ignoreFlagDescriptorChecksum;
    }

    /**
     * Reads the magic number and frame descriptor from the underlying {@link InputStream}.
     *
     * @throws IOException
     */
    private void readHeader() throws IOException {
        // read first 6 bytes into buffer to check magic and FLG/BD descriptor flags
        int headerOffset = 6;
        if (in.remaining() < headerOffset) {
            throw new IOException(PREMATURE_EOS);
        }

        if (MAGIC != Utils.readUnsignedInt(in, 0)) {
            throw new IOException(NOT_SUPPORTED);
        }
        flg = FLG.fromByte(in.get(4));
        bd = BD.fromByte(in.get(5));

        if (flg.isContentSizeSet()) {
            if (in.remaining() < 8)
                throw new IOException(PREMATURE_EOS);
            headerOffset += 8;
        }

        // Final byte of Frame Descriptor is HC checksum
        headerOffset++;

        in.position(headerOffset);

        // Old implementations produced incorrect HC checksums
        if (ignoreFlagDescriptorChecksum)
            return;

        int offset = 4;
        int len = headerOffset - offset - 1; // don't include magic bytes or HC that was in and out

        final int fullHash = in.hasArray() ?
                             // workaround for https://github.com/lz4/lz4-java/pull/65
                             CHECKSUM.hash(in.array(), in.arrayOffset() + offset, len, 0) :
                             CHECKSUM.hash(in, offset, len, 0);
        byte hash = (byte) ((fullHash >> 8) & 0xFF);
        if (hash != in.get(headerOffset - 1))
            throw new IOException(DESCRIPTOR_HASH_MISMATCH);
        in.position(headerOffset);

    }

    /**
     * Decompresses (if necessary) buffered data, optionally computes and validates a XXHash32 checksum, and writes the
     * result to a buffer.
     *
     * @throws IOException
     */
    private void readBlock() throws IOException {
        int blockSize = (int) Utils.readUnsignedInt(in);

        // Check for EndMark
        if (blockSize == 0) {
            finished = true;
            if (flg.isContentChecksumSet())
                Utils.readUnsignedInt(in); // TODO: verify this content checksum
            return;
        } else if (blockSize > maxBlockSize) {
            throw new IOException(String.format("Block size %s exceeded max: %s",
                                                blockSize,
                                                maxBlockSize
            ));
        }

        boolean compressed = (blockSize & LZ4_FRAME_INCOMPRESSIBLE_MASK) == 0;
        if (compressed) {
            _buffer = decompressionBuffer;
        } else {
            blockSize &= ~LZ4_FRAME_INCOMPRESSIBLE_MASK;
            _buffer = in.slice();
            _buffer.limit(blockSize);
        }

        if (in.remaining() < blockSize) {
            throw new IOException(PREMATURE_EOS);
        }

        // verify checksum
        if (flg.isBlockChecksumSet() && Utils.readUnsignedInt(in, in.position() + blockSize + 1) != CHECKSUM
            .hash(in, in.position(), blockSize, 0)) {
            throw new IOException(BLOCK_HASH_MISMATCH);
        }

        if (compressed) {
            try {
                // workaround for https://github.com/lz4/lz4-java/pull/65
                final int bufferSize;
                if (in.hasArray()) {
                    bufferSize = DECOMPRESSOR.decompress(
                        in.array(),
                        in.position() + in.arrayOffset(),
                        blockSize,
                        _buffer.array(),
                        0,
                        maxBlockSize
                    );
                } else {
                    // degenerate case, fall back to copying bytes
                    byte[] bytesIn = new byte[blockSize];
                    in.duplicate().get(bytesIn);
                    bufferSize = DECOMPRESSOR.decompress(bytesIn, 0, blockSize, _buffer.array(), _buffer.arrayOffset(), maxBlockSize);
                }
                _buffer.limit(bufferSize);
            } catch (LZ4Exception e) {
                throw new IOException(e);
            }
        }

        in.position(in.position() + blockSize + (flg.isBlockChecksumSet() ? 1 : 0));
    }

    @Override
    public int read() throws IOException {
        if (finished) {
            return -1;
        }
        if (available() == 0) {
            readBlock();
        }
        if (finished) {
            return -1;
        }

        return _buffer.get() & 0xFF;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        net.jpountz.util.SafeUtils.checkRange(b, off, len);
        if (finished) {
            return -1;
        }
        if (available() == 0) {
            readBlock();
        }
        if (finished) {
            return -1;
        }
        len = Math.min(len, available());

        _buffer.get(b, off, len);
        return len;
    }

    @Override
    public long skip(long n) throws IOException {
        if (finished) {
            return 0;
        }
        if (available() == 0) {
            readBlock();
        }
        if (finished) {
            return 0;
        }
        int skipped = (int) Math.min(n, available());
        _buffer.position(_buffer.position() + skipped);
        return n;
    }

    @Override
    public int available() throws IOException {
        return _buffer == null ? 0 : _buffer.remaining();
    }

    @Override
    public void close() throws IOException {
        FREE.deallocate(decompressionBuffer);
    }

    @Override
    public synchronized void mark(int readlimit) {
        throw new RuntimeException("mark not supported");
    }

    @Override
    public synchronized void reset() throws IOException {
        throw new RuntimeException("reset not supported");
    }

    @Override
    public boolean markSupported() {
        return false;
    }

}
