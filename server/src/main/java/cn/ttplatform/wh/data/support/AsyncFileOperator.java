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
import java.nio.channels.FileChannel;

import static cn.ttplatform.wh.constant.ErrorMessage.INDEX_OUT_OF_BOUND;
import static java.nio.file.StandardOpenOption.*;

/**
 * @author Wang Hao
 * @date 2021/6/28 17:26
 */
@Slf4j
public class AsyncFileOperator {

    private final File file;
    private long fileSize;
    private final int blockSize;
    private final FlushStrategy flushStrategy;
    private final FileChannel fileChannel;
    private final Pool<ByteBuffer> byteBufferPool;
    private final LRU<Long, Block> blocks;
    private FileHeaderOperator headerOperator;

    public AsyncFileOperator(ServerProperties properties, Pool<ByteBuffer> byteBufferPool, File file, FileHeaderOperator headerOperator) {
        this.file = file;
        this.byteBufferPool = byteBufferPool;
        try {
            this.fileChannel = FileChannel.open(file.toPath(), READ, WRITE, CREATE);
            this.blockSize = properties.getBlockSize();
            this.headerOperator = headerOperator;
            this.blocks = new LRU<>(properties.getBlockCacheSize());
            this.flushStrategy = new PriorityFlushStrategy(file.getName(), properties.getBlockFlushInterval());
            this.fileSize = headerOperator.getFileSize();
            log.info("open the file[{}], and the file size is {}.", file, fileSize);
        } catch (IOException e) {
            throw new OperateFileException("failed to open the file channel.", e);
        }
    }

    public void changeFileHeaderOperator(FileHeaderOperator headerOperator) {
        this.headerOperator = headerOperator;
        this.fileSize = headerOperator.getFileSize();
    }

    public void updateFileSize(int increment) {
        fileSize += increment;
        headerOperator.recordFileSize(fileSize);
    }

    /**
     * start offset of a block is block id.
     *
     * @param position position exist in [start, end].
     * @return block id
     */
    private long calculateBlockId(long position) {
        return position / blockSize * blockSize;
    }

    private boolean inPool(long blockId) {
        return blocks.get(blockId) != null;
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

    public ByteBuffer[] readBytes(long position) {
        if (position < 0 || position >= fileSize) {
            throw new IllegalArgumentException(String.format(INDEX_OUT_OF_BOUND, position, 0, fileSize));
        }
        int total = (int) (fileSize - position);
        int bufferSize = (total % blockSize) == 0 ? total / blockSize : total / blockSize + 1;
        ByteBuffer[] byteBuffers = new ByteBuffer[bufferSize];
        int index = 0;
        ByteBuffer byteBuffer = byteBufferPool.allocate();
        while (position < fileSize) {
            if (!byteBuffer.hasRemaining()) {
                byteBuffers[index++] = byteBuffer;
                byteBuffer = byteBufferPool.allocate();
            }
            while (position < fileSize && byteBuffer.hasRemaining()) {
                Block block = getBlock(position);
                position += block.get((int) (position - block.startOffset), byteBuffer, (int) (fileSize - position));
            }
        }
        if (index == byteBuffers.length - 1) {
            byteBuffer.limit(total - blockSize * index);
        }
        byteBuffers[index] = byteBuffer;
        return byteBuffers;
    }

    public void appendBlock(ByteBuffer byteBuffer) {
        if (byteBuffer.capacity() != blockSize) {
            throw new IllegalArgumentException(String.format("byteBuffer's capacity must be %d, but is %d.", blockSize, byteBuffer.capacity()));
        }
        int limit = byteBuffer.limit();
        addBlock(fileSize, new Block(fileSize, byteBuffer));
        updateFileSize(limit);
        byteBuffer.limit(byteBuffer.capacity());
    }

    public void appendBytes(ByteBuffer byteBuffer) {
        int offset = 0;
        byteBuffer.position(0);
        while (byteBuffer.hasRemaining()) {
            Block block = getBlock(fileSize);
            block.put((int) (fileSize - block.startOffset), byteBuffer);
            flushStrategy.flush(block);
            fileSize = fileSize + byteBuffer.position() - offset;
            offset = byteBuffer.position();
        }
        updateFileSize(0);
    }

    public void appendBytes(byte[] bytes) {
        int offset = 0;
        while (offset < bytes.length) {
            Block block = getBlock(fileSize);
            int position = (int) (fileSize - block.startOffset);
            int length = Math.min(bytes.length - offset, blockSize - position);
            block.put(position, bytes, offset, length);
            flushStrategy.flush(block);
            offset += length;
            fileSize += length;
        }
        updateFileSize(0);
    }

    public void write(long position, byte b) {
        Block block = getBlock(position);
        block.put((int) (position - block.startOffset), b);
        flushStrategy.flush(block);
    }

    public void appendInt(int v) {
        write(fileSize, Bits.int3(v));
        fileSize++;
        write(fileSize, Bits.int2(v));
        fileSize++;
        write(fileSize, Bits.int1(v));
        fileSize++;
        write(fileSize, Bits.int0(v));
        updateFileSize(1);
    }


    public void appendLong(long v) {
        write(fileSize, Bits.long7(v));
        fileSize++;
        write(fileSize, Bits.long6(v));
        fileSize++;
        write(fileSize, Bits.long5(v));
        fileSize++;
        write(fileSize, Bits.long4(v));
        fileSize++;
        write(fileSize, Bits.long3(v));
        fileSize++;
        write(fileSize, Bits.long2(v));
        fileSize++;
        write(fileSize, Bits.long1(v));
        fileSize++;
        write(fileSize, Bits.long0(v));
        updateFileSize(1);
    }

    public byte read(long position) {
        Block block = getBlock(position);
        return block.get((int) (position - block.startOffset));
    }

    public int getInt(long position) {
        if (position >= fileSize) {
            throw new IllegalArgumentException(String.format("position[%d] can not greater than %d.", position, fileSize));
        }
        return Bits.makeInt(read(position++), read(position++), read(position++), read(position));
    }

    public long getLong(long position) {
        if (position >= fileSize) {
            throw new IllegalArgumentException(String.format("position[%d] can not greater than %d.", position, fileSize));
        }
        return Bits.makeLong(read(position++), read(position++), read(position++), read(position++),
                read(position++), read(position++), read(position++), read(position));
    }

    public void get(long position, byte[] bytes) {
        if (position < 0 || position > fileSize - bytes.length) {
            throw new IllegalArgumentException(String.format("required %d bytes, position[%d] out of bound [%d,%d].", fileSize - bytes.length, position, 0, fileSize - 1));
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

    public void truncate(long position) {
        long blockId = calculateBlockId(position + blockSize);
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
            fileChannel.truncate(position);
        } catch (IOException e) {
            throw new OperateFileException(String.format("failed to truncate a file at offset[%d].", position), e);
        }
        updateFileSize((int) (position - fileSize));
    }

    public long getSize() {
        return fileSize;
    }

    public boolean isEmpty() {
        return fileSize <= 0;
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
            this.startOffset = offset / blockSize * blockSize;
            this.endOffset = startOffset + blockSize - 1;
            this.byteBuffer = byteBufferPool.allocate();
            if (fileSize > startOffset) {
                load();
            }
            this.state = 0x07;
            log.info("create a block[{},{}] for {}.", startOffset, endOffset, file);
        }

        public Block(long startOffset, ByteBuffer buffer) {
            this.startOffset = startOffset;
            this.endOffset = startOffset + buffer.capacity() - 1;
            buffer.limit(buffer.capacity());
            this.byteBuffer = buffer;
            this.state = 0x07;
            log.info("create a block[{},{}] for {}.", startOffset, endOffset, file);
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
//                    fileChannel.force(true);
                    log.info("async flush a dirty block[{}] into {}.", this, file);
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

        public synchronized int get(int position, ByteBuffer destination, int length) {
            byteBuffer.position(position);
            if (destination.remaining() < byteBuffer.remaining()) {
                length = Math.min(destination.remaining(), length);
            } else {
                length = Math.min(byteBuffer.remaining(), length);
            }
            byteBuffer.limit(byteBuffer.position() + length);
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
