package cn.ttplatform.wh.data.pool;

import cn.ttplatform.wh.GlobalContext;
import cn.ttplatform.wh.config.ServerProperties;
import cn.ttplatform.wh.data.FileConstant;
import cn.ttplatform.wh.data.pool.strategy.FlushStrategy;
import cn.ttplatform.wh.data.pool.strategy.PriorityFlushStrategy;
import cn.ttplatform.wh.data.tool.Bits;
import cn.ttplatform.wh.exception.OperateFileException;
import cn.ttplatform.wh.support.LRU;
import cn.ttplatform.wh.support.LRU.KVEntry;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import static java.nio.file.StandardOpenOption.*;

/**
 * @author Wang Hao
 * @date 2021/6/8 10:03
 */
@Slf4j
public class BlockCache {

    private final int dataOffset;
    private long fileSize;
    private final int blockSize;
    private final LRU<Long, Block> blocks;
    private final FlushStrategy flushStrategy;
    private final ByteBuffer header;
    private final FileChannel fileChannel;

    public BlockCache(int blockCacheSize, int blockSize, long blockFlushInterval, File file, int dataOffset) {
        this.blocks = new LRU<>(blockCacheSize);
        this.blockSize = blockSize;
        try {
            this.fileChannel = FileChannel.open(file.toPath(), READ, WRITE, CREATE, DSYNC);
            log.info("open file[{}].", file);
            this.dataOffset = dataOffset;
            this.header = fileChannel.map(MapMode.READ_WRITE, 0, dataOffset);
            header.position(0);
            this.fileSize = Bits.getLong(header);
            if (fileSize < dataOffset) {
                updateFileSize(dataOffset);
            }
            this.flushStrategy = new PriorityFlushStrategy(blockFlushInterval);
        } catch (IOException e) {
            throw new OperateFileException("open file channel error.", e);
        }
    }

    public void updateFileSize(int increment) {
        fileSize += increment;
        header.clear();
        Bits.putLong(fileSize, header);
    }

    /**
     * start offset of a block is block id.
     *
     * @param position position exist in [start, end].
     * @return block id
     */
    public long calculateBlockId(long position) {
        return (position - dataOffset) / blockSize * blockSize + dataOffset;
    }

    public boolean inPool(long blockId) {
        return blocks.get(blockId) != null;
    }

    private Block getBlock(long position) {
        long blockId = calculateBlockId(position);
//        if (blockId > fileSize) {
//            throw new IndexOutOfBoundsException(
//                    String.format("index[%d] is greater than fileSize[%d].", blockId, fileSize));
//        }
        Block block = blocks.get(blockId);
        if (block == null) {
            // fetch a block from buffer pool, if the block not exist in pool, we will load it from disk, then put it into buffer pool.
            block = new Block(blockId);
            addBlock(blockId, block);
        }
        return block;
    }

    public ByteBuffer[] getBlocks(long position) {
        ByteBuffer[] byteBuffers = new ByteBuffer[(int) ((fileSize - position) / blockSize) + 1];
        int index = 0;
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(blockSize);
        while (position < fileSize) {
            if (!byteBuffer.hasRemaining()) {
                byteBuffers[index++] = byteBuffer;
                byteBuffer = ByteBuffer.allocateDirect(blockSize);
            }
            while (position < fileSize && byteBuffer.hasRemaining()) {
                Block block = getBlock(position);
                position += block.get((int) (position - block.startOffset), byteBuffer);
            }
        }
        byteBuffers[index] = byteBuffer;
        return byteBuffers;
    }

    private void addBlock(long blockId, Block block) {
        // put a block into buffer pool
        KVEntry<Long, Block> kvEntry = blocks.put(blockId, block);
        if (kvEntry != null) {
            // if pool is full, a latest recent used block will be removed.
            Block removed = kvEntry.getValue();
            if (removed.dirty()) {
                // if the removed block is dirty, it will be flushed into disk late.
                flushStrategy.flush(removed);
            }
        }
    }

    public void appendBlock(ByteBuffer byteBuffer) {
        addBlock(fileSize, new Block(fileSize, byteBuffer));
        updateFileSize(byteBuffer.capacity());
    }

    public void appendByteBuffer(ByteBuffer byteBuffer) {
        int offset = 0;
        byteBuffer.flip();
        while (byteBuffer.hasRemaining()) {
            Block block = getBlock(fileSize);
            block.put((int) (fileSize - block.startOffset), byteBuffer);
            fileSize = fileSize + byteBuffer.position() - offset;
            offset = byteBuffer.position();
            flushStrategy.flush(block);
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
            offset += length;
            fileSize += length;
            flushStrategy.flush(block);
        }
        updateFileSize(0);
    }

    public void appendInt(int v) {
        put(fileSize, Bits.int3(v));
        fileSize++;
        put(fileSize, Bits.int2(v));
        fileSize++;
        put(fileSize, Bits.int1(v));
        fileSize++;
        put(fileSize, Bits.int0(v));
        updateFileSize(1);
    }

    public void appendLong(long v) {
        put(fileSize, Bits.long7(v));
        fileSize++;
        put(fileSize, Bits.long6(v));
        fileSize++;
        put(fileSize, Bits.long5(v));
        fileSize++;
        put(fileSize, Bits.long4(v));
        fileSize++;
        put(fileSize, Bits.long3(v));
        fileSize++;
        put(fileSize, Bits.long2(v));
        fileSize++;
        put(fileSize, Bits.long1(v));
        fileSize++;
        put(fileSize, Bits.long0(v));
        updateFileSize(1);
    }

    public void put(long position, byte b) {
        Block block = getBlock(position);
        block.put((int) (position - block.startOffset), b);
        flushStrategy.flush(block);
    }

    public int getInt(long position) {
        if (position >= fileSize) {
            throw new IllegalArgumentException("position[" + position + "] can not greater than " + fileSize + ".");
        }
        return Bits.makeInt(get(position++), get(position++), get(position++), get(position));
    }

    public long getLong(long position) {
        if (position >= fileSize) {
            throw new IllegalArgumentException("position[" + position + "] can not greater than " + fileSize + ".");
        }
        return Bits.makeLong(get(position++), get(position++), get(position++), get(position++), get(position++),
                get(position++),
                get(position++), get(position));
    }

    public byte get(long position) {
        if (position >= fileSize) {
            throw new IllegalArgumentException("position[" + position + "] can not greater than " + fileSize + ".");
        }
        Block block = getBlock(position);
        return block.get((int) (position - block.startOffset));
    }

    public void get(long position, byte[] bytes) {
        if (position >= fileSize) {
            throw new IllegalArgumentException("position[" + position + "] can not greater than " + fileSize + ".");
        }
        int offset = 0;
        while (offset < bytes.length && position < fileSize) {
            Block block = getBlock(position);
            int length = Math.min(bytes.length - offset, (int) (block.endOffset - position));
            block.get((int) (position - block.startOffset), bytes, offset, length);
            offset += length;
            position += length;
        }
    }

    public void removeAfter(long offset) {
        long blockId = calculateBlockId(offset);
        // remove all block that startOffset > offset in buffer pool.
        while (blockId < fileSize) {
            if (inPool(blockId)) {
                blocks.remove(blockId);
            }
            blockId += blockSize;
        }
        // remove all matched content in disk.
        try {
            fileChannel.truncate(offset);
        } catch (IOException e) {
            throw new OperateFileException(String.format("truncate a file[index=%d] error", offset), e);
        }
        updateFileSize((int) (offset - fileSize));
    }

    public long getSize() {
        return fileSize;
    }

    public void close() {
        flushStrategy.synFlushAll();
    }

    @ToString
    @EqualsAndHashCode
    public class Block {

        long startOffset;
        long endOffset;
        volatile boolean dirty;
        ByteBuffer byteBuffer;

        public Block(long offset) {
            this.startOffset = (offset - dataOffset) / blockSize * blockSize + dataOffset;
            this.endOffset = startOffset + blockSize - 1;
            this.byteBuffer = ByteBuffer.allocateDirect(blockSize);
            if (fileSize > startOffset) {
//                byteBuffer.limit(Math.min((int) (fileSize - startOffset), blockSize));
                load();
            }
        }

        public Block(long startOffset, ByteBuffer buffer) {
            this.startOffset = startOffset;
            this.endOffset = buffer.capacity();
            this.byteBuffer = buffer;
            this.dirty = true;
        }

        /**
         * load the virtual data from disk.
         */
        public void load() {
            try {
                fileChannel.read(byteBuffer, startOffset);
            } catch (IOException e) {
                throw new OperateFileException("read an byte[] data to file[index(" + startOffset + ")] error");
            }
        }

        public void flush() {
            try {
                synchronized (this) {
                    byteBuffer.position(0);
                    byteBuffer.limit(byteBuffer.capacity());
                    fileChannel.write(byteBuffer, startOffset);
                }
                fileChannel.force(true);
                dirty = false;
            } catch (IOException e) {
                throw new OperateFileException("write an byte[] data to file[index(" + startOffset + ")] error");
            }
        }

        public synchronized void put(int position, ByteBuffer source) {
            byteBuffer.position(position);
            if (source.remaining() > byteBuffer.remaining()) {
                source.limit(source.position() + byteBuffer.remaining());
            }
            byteBuffer.put(source);
            source.limit(source.capacity());
            dirty = true;
        }

        public synchronized void put(int position, byte[] src, int offset, int length) {
            byteBuffer.position(position);
            byteBuffer.put(src, offset, length);
            dirty = true;
        }

        public synchronized void put(int position, byte b) {
            byteBuffer.position(position);
            byteBuffer.put(b);
            dirty = true;
        }

        public synchronized int get(int position, ByteBuffer destination) {
            byteBuffer.position(position);
            int length;
            if (destination.remaining() < byteBuffer.remaining()) {
                length = destination.remaining();
                byteBuffer.limit(byteBuffer.position() + destination.remaining());
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

        public boolean dirty() {
            return dirty;
        }

        public long getStartOffset() {
            return startOffset;
        }

    }

}
