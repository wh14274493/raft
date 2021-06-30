package cn.ttplatform.wh.data.support;

import cn.ttplatform.wh.config.ServerProperties;
import cn.ttplatform.wh.exception.OperateFileException;
import cn.ttplatform.wh.support.LRU;
import cn.ttplatform.wh.support.Pool;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import static cn.ttplatform.wh.constant.ErrorMessage.INDEX_OUT_OF_BOUND;
import static java.nio.file.StandardOpenOption.*;

/**
 * @author Wang Hao
 * @date 2021/6/28 17:26
 */
@Slf4j
public class AsyncFileOperator {

    private final int dataOffset;
    private long fileSize;
    private final int blockSize;
    private final FlushStrategy flushStrategy;
    private final FileChannel fileChannel;
    private final Pool<ByteBuffer> byteBufferPool;
    private final FileHeaderOperator headerOperator;
    private final FileBodyOperator bodyOperator;

    private class FileHeaderOperator {
        private final MappedByteBuffer header;
        private final long endOffset;

        public FileHeaderOperator(int dataOffset) throws IOException {
            this.endOffset = dataOffset;
            // endOffset is excluded in header.
            this.header = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, endOffset);
        }

        public void clear() {
            header.position(0);
            Bits.putLong(endOffset, header);
            while (header.hasRemaining()) {
                header.put((byte) 0);
            }
        }

        public void writeInt(long position, int v) {
            if (position < 0 || position > endOffset - Integer.BYTES) {
                throw new IllegalArgumentException(String.format(INDEX_OUT_OF_BOUND, position, 0, endOffset - Integer.BYTES));
            }
            header.position((int) position);
            Bits.putInt(v, header);
        }

        public void writeLong(long position, long v) {
            if (position < 0 || position > endOffset - Long.BYTES) {
                throw new IllegalArgumentException(String.format(INDEX_OUT_OF_BOUND, position, 0, endOffset - Long.BYTES));
            }
            header.position((int) position);
            Bits.putLong(v, header);
        }

        public int readInt(long position) {
            if (position < 0 || position > endOffset - Integer.BYTES) {
                throw new IllegalArgumentException(String.format(INDEX_OUT_OF_BOUND, position, 0, endOffset - Integer.BYTES));
            }
            header.position((int) position);
            return Bits.getInt(header);
        }

        public long readLong(long position) {
            if (position < 0 || position > endOffset - Long.BYTES) {
                throw new IllegalArgumentException(String.format(INDEX_OUT_OF_BOUND, position, 0, endOffset - Long.BYTES));
            }
            header.position((int) position);
            return Bits.getLong(header);
        }

        public void force() {
            header.force();
        }
    }

    private class FileBodyOperator {

        private final long startOffset;
        private final int blockSize;
        private final LRU<Long, Block> blocks;
        private final FlushStrategy flushStrategy;

        public FileBodyOperator(long startOffset, int blockSize, int blockCacheSize, long blockFlushInterval) {
            this.startOffset = startOffset;
            this.blockSize = blockSize;
            this.blocks = new LRU<>(blockCacheSize);
            this.flushStrategy = new PriorityFlushStrategy(blockFlushInterval);
        }

        /**
         * start offset of a block is block id.
         *
         * @param position position exist in [start, end].
         * @return block id
         */
        private long calculateBlockId(long position) {
            return (position - dataOffset) / blockSize * blockSize + dataOffset;
        }

        private boolean inPool(long blockId) {
            return blocks.get(blockId) != null;
        }

        private Block getBlock(long position) {
            long blockId = calculateBlockId(position);
            Block block = blocks.get(blockId);
            if (block == null) {
                // fetch a block from buffer pool, if the block not exist in pool, we will load it from disk, then put it into buffer pool.
                block = new Block(blockId);
                addBlock(blockId, block);
            }
            return block;
        }

        private void addBlock(long blockId, Block block) {
            // put a block into buffer pool
            LRU.KVEntry<Long, Block> kvEntry = blocks.put(blockId, block);
            if (kvEntry != null) {
                // if pool is full, a latest recent used block will be removed.
                Block removed = kvEntry.getValue();
                removed.cancelExistBit();
                if (removed.dirty()) {
                    // if the removed block is dirty, it will be flushed into disk late.
                    flushStrategy.flush(removed);
                }
            }
            flushStrategy.flush(block);
        }

        public void writeBytes(long position, ByteBuffer byteBuffer) {
            if (position < this.startOffset || position > fileSize) {
                throw new IllegalArgumentException(String.format(INDEX_OUT_OF_BOUND, startOffset, this.startOffset, fileSize));
            }
            Block block = getBlock(position);
            block.put((int) (position - block.startOffset), byteBuffer);
            flushStrategy.flush(block);
        }

        public int writeBytes(long position, byte[] bytes, int offset) {
            if (position < this.startOffset || position > fileSize) {
                throw new IllegalArgumentException(String.format(INDEX_OUT_OF_BOUND, startOffset, this.startOffset, fileSize));
            }
            Block block = getBlock(position);
            position = position - block.startOffset;
            int length = Math.min(bytes.length - offset, (int) (blockSize - position));
            block.put((int) position, bytes, offset, length);
            flushStrategy.flush(block);
            return length;
        }

        public ByteBuffer[] read(long startOffset, long endOffset) {
            if (startOffset < this.startOffset || startOffset >= endOffset) {
                throw new IllegalArgumentException(String.format(INDEX_OUT_OF_BOUND, startOffset, this.startOffset, endOffset));
            }
            int total = (int) (endOffset - startOffset);
            int bufferSize = (total & (blockSize - 1)) == 0 ? total / blockSize : total / blockSize + 1;
            ByteBuffer[] byteBuffers = new ByteBuffer[bufferSize];
            int index = 0;
            ByteBuffer byteBuffer = byteBufferPool.allocate();
            while (startOffset < endOffset) {
                if (!byteBuffer.hasRemaining()) {
                    byteBuffers[index++] = byteBuffer;
                    byteBuffer = byteBufferPool.allocate();
                    if (index == byteBuffers.length - 1) {
                        byteBuffer.limit(total - blockSize * index);
                    }
                }
                while (startOffset < endOffset && byteBuffer.hasRemaining()) {
                    Block block = getBlock(startOffset);
                    startOffset += block.get((int) (startOffset - block.startOffset), byteBuffer);
                }
            }
            byteBuffers[index] = byteBuffer;
            return byteBuffers;
        }

        public void write(long position, byte b) {
            if (position < this.startOffset || position > fileSize) {
                throw new IllegalArgumentException(String.format(INDEX_OUT_OF_BOUND, startOffset, this.startOffset, fileSize - 1));
            }
            Block block = getBlock(position);
            block.put((int) (position - block.startOffset), b);
            flushStrategy.flush(block);
        }

        public byte read(long position) {
            if (position < startOffset || position > fileSize - 1) {
                throw new IllegalArgumentException(String.format("required %d bytes, position[%d] out of bound [%d,%d].", fileSize - Long.BYTES, position, startOffset, fileSize - 1));
            }
            Block block = getBlock(position);
            return block.get((int) (position - block.startOffset));
        }


        public void read(long position, byte[] bytes) {
            if (position < startOffset || position > fileSize - bytes.length) {
                throw new IllegalArgumentException(String.format("required %d bytes, position[%d] out of bound [%d,%d].", fileSize - bytes.length, position, startOffset, fileSize - 1));
            }
            int offset = 0;
            while (offset < bytes.length && position < fileSize) {
                Block block = getBlock(position);
                int length = Math.min(bytes.length - offset, (int) (block.endOffset - position + 1));
                block.get((int) (position - block.startOffset), bytes, offset, length);
                offset += length;
                position += length;
            }
        }

        public void truncate(long offset) {
            long blockId = calculateBlockId(offset + blockSize);
            // remove all block that startOffset > offset in buffer pool.
            while (blockId < fileSize) {
                if (inPool(blockId)) {
                    LRU.KVEntry<Long, Block> remove = blocks.remove(blockId);
                    Block block = remove.getValue();
                    block.cancelValidBit();
                    block.cancelExistBit();
                }
                blockId += blockSize;
            }
            // remove all matched content in disk.
            try {
                fileChannel.truncate(offset);
            } catch (IOException e) {
                throw new OperateFileException(String.format("failed to truncate a file at offset[%d].", offset), e);
            }
        }
    }

    public AsyncFileOperator(ServerProperties properties, Pool<ByteBuffer> byteBufferPool, File file, int dataOffset) {
        this.byteBufferPool = byteBufferPool;
        try {
            this.fileChannel = FileChannel.open(file.toPath(), READ, WRITE, CREATE, DSYNC);
            this.blockSize = properties.getBlockSize();
            this.headerOperator = new FileHeaderOperator(dataOffset);
            this.bodyOperator = new FileBodyOperator(dataOffset, blockSize, properties.getBlockCacheSize(), properties.getBlockFlushInterval());
            log.info("open the file[{}].", file);
            this.dataOffset = dataOffset;
            this.fileSize = headerOperator.readLong(0L);
            if (fileSize < dataOffset) {
                updateFileSize((int) (dataOffset - fileSize));
            }
            this.flushStrategy = new PriorityFlushStrategy(properties.getBlockFlushInterval());
        } catch (IOException e) {
            throw new OperateFileException("failed to open the file channel.", e);
        }
    }

    public void updateFileSize(int increment) {
        fileSize += increment;
        if (fileSize == dataOffset) {
            headerOperator.clear();
        } else {
            headerOperator.writeLong(0, fileSize);
        }
    }

    public void writeHeader(long position, int v) {
        headerOperator.writeInt(position, v);
    }

    public void writeHeader(long position, long v) {
        headerOperator.writeLong(position, v);
    }

    public int readIntFromHeader(long position) {
        return headerOperator.readInt(position);
    }

    public long readLongFromHeader(long position) {
        return headerOperator.readLong(position);
    }

    public ByteBuffer[] read(long position) {
        return bodyOperator.read(position, fileSize);
    }

    public void appendBlock(ByteBuffer byteBuffer) {
        if (byteBuffer.capacity() != blockSize) {
            throw new IllegalArgumentException(String.format("byteBuffer's capacity must be %d, but is %d.", blockSize, byteBuffer.capacity()));
        }
        bodyOperator.addBlock(fileSize, new Block(fileSize, byteBuffer));
        updateFileSize(byteBuffer.limit());
        byteBuffer.limit(byteBuffer.capacity());
    }

    public void appendBytes(ByteBuffer byteBuffer) {
        int offset = 0;
        byteBuffer.position(0);
        while (byteBuffer.hasRemaining()) {
            bodyOperator.writeBytes(fileSize, byteBuffer);
            fileSize = fileSize + byteBuffer.position() - offset;
            offset = byteBuffer.position();
        }
        updateFileSize(0);
    }

    public void appendBytes(byte[] bytes) {
        int offset = 0;
        while (offset < bytes.length) {
            int writeLength = bodyOperator.writeBytes(fileSize, bytes, offset);
            offset += writeLength;
            fileSize += writeLength;
        }
        updateFileSize(0);
    }

    public void appendInt(int v) {
        bodyOperator.write(fileSize, Bits.int3(v));
        fileSize++;
        bodyOperator.write(fileSize, Bits.int2(v));
        fileSize++;
        bodyOperator.write(fileSize, Bits.int1(v));
        fileSize++;
        bodyOperator.write(fileSize, Bits.int0(v));
        updateFileSize(1);
    }


    public void appendLong(long v) {
        bodyOperator.write(fileSize, Bits.long7(v));
        fileSize++;
        bodyOperator.write(fileSize, Bits.long6(v));
        fileSize++;
        bodyOperator.write(fileSize, Bits.long5(v));
        fileSize++;
        bodyOperator.write(fileSize, Bits.long4(v));
        fileSize++;
        bodyOperator.write(fileSize, Bits.long3(v));
        fileSize++;
        bodyOperator.write(fileSize, Bits.long2(v));
        fileSize++;
        bodyOperator.write(fileSize, Bits.long1(v));
        fileSize++;
        bodyOperator.write(fileSize, Bits.long0(v));
        updateFileSize(1);
    }


    public int getInt(long position) {
        if (position >= fileSize) {
            throw new IllegalArgumentException(String.format("position[%d] can not greater than %d.", position, fileSize));
        }
        return Bits.makeInt(bodyOperator.read(position++), bodyOperator.read(position++), bodyOperator.read(position++), bodyOperator.read(position));
    }

    public long getLong(long position) {
        if (position >= fileSize) {
            throw new IllegalArgumentException(String.format("position[%d] can not greater than %d.", position, fileSize));
        }
        return Bits.makeLong(bodyOperator.read(position++), bodyOperator.read(position++), bodyOperator.read(position++), bodyOperator.read(position++),
                bodyOperator.read(position++), bodyOperator.read(position++), bodyOperator.read(position++), bodyOperator.read(position));
    }

    public void get(long position, byte[] bytes) {
        bodyOperator.read(position, bytes);
    }

    public void truncate(long position) {
        bodyOperator.truncate(position);
        updateFileSize((int) (position - fileSize));
    }

    public long getSize() {
        return fileSize;
    }

    public boolean isEmpty() {
        return fileSize <= dataOffset;
    }

    public void close() {
        flushStrategy.synFlushAll();
        headerOperator.force();
    }

    @ToString
    @EqualsAndHashCode
    public class Block {

        long startOffset;
        long endOffset;
        /**
         * use a byte record state info
         * valid
         * exist
         * dirty
         * 0    0   0   0   0   0   0   0
         */
        volatile byte state;
        ByteBuffer byteBuffer;

        public Block(long offset) {
            this.startOffset = (offset - dataOffset) / blockSize * blockSize + dataOffset;
            this.endOffset = startOffset + blockSize - 1;
            this.byteBuffer = byteBufferPool.allocate();
            if (fileSize > startOffset) {
                load();
            }
            this.state = 0x07;
        }

        public Block(long startOffset, ByteBuffer buffer) {
            this.startOffset = startOffset;
            this.endOffset = buffer.capacity();
            this.byteBuffer = buffer;
            this.state = 0x07;
        }

        public void load() {
            try {
                // load the virtual data from disk.
                fileChannel.read(byteBuffer, startOffset);
            } catch (IOException e) {
                throw new OperateFileException(String.format("failed to read %d bytes from file at offset[%d].", byteBuffer.limit(), startOffset), e);
            }
        }

        public void flush() {
            try {
                if (valid()) {
                    synchronized (this) {
                        byteBuffer.position(0);
                        byteBuffer.limit(byteBuffer.capacity());
                        fileChannel.write(byteBuffer, startOffset);
                        state = (byte) (state >>> 1 << 1);
                    }
                    fileChannel.force(true);
                }
            } catch (IOException e) {
                throw new OperateFileException(String.format("failed to write %d bytes into file at offset[%d].", byteBuffer.limit(), startOffset), e);
            } finally {
                if (!exist()) {
                    // the block had been removed from block pool, so we need recycle this block.
                    recycle();
                }
            }
        }

        public synchronized void put(int position, ByteBuffer source) {
            byteBuffer.position(position);
            int limit = source.limit();
            if (source.remaining() > byteBuffer.remaining()) {
                source.limit(source.position() + byteBuffer.remaining());
            }
            byteBuffer.put(source);
            source.limit(limit);
            state |= 0x07;
        }

        public synchronized void put(int position, byte[] src, int offset, int length) {
            byteBuffer.position(position);
            byteBuffer.put(src, offset, length);
            state |= 0x07;
        }

        public synchronized void put(int position, byte b) {
            byteBuffer.position(position);
            byteBuffer.put(b);
            state |= 0x07;
        }

        public synchronized int get(int position, ByteBuffer destination) {
            byteBuffer.position(position);
            int length;
            if (destination.remaining() < byteBuffer.remaining()) {
                length = destination.remaining();
                byteBuffer.limit(byteBuffer.position() + length);
            } else {
                length = byteBuffer.remaining();
            }
            destination.put(byteBuffer);
            byteBuffer.limit(byteBuffer.capacity());
            return length;
        }

        public synchronized void get(int position, byte[] dst, int offset, int length) {
            byteBuffer.position(position);
            byteBuffer.get(dst, offset, length);
        }

        public synchronized byte get(int position) {
            byteBuffer.position(position);
            return byteBuffer.get();
        }

        public synchronized ByteBuffer getByteBuffer() {
            return byteBuffer;
        }

        public boolean valid() {
            return (state & 0x04) == 0x04;
        }

        public boolean dirty() {
            return (state & 0x01) == 0x01;
        }

        public boolean exist() {
            return (state & 0x02) == 0x02;
        }

        public synchronized void cancelValidBit() {
            state = (byte) ((state >>> 3) | (state << 6));
        }

        public synchronized void cancelExistBit() {
            state = (byte) ((state >>> 2) | (state << 7));
        }

        public long getStartOffset() {
            return startOffset;
        }

        public void recycle() {
            byteBufferPool.recycle(byteBuffer);
        }
    }
}
