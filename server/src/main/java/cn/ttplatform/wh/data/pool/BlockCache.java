package cn.ttplatform.wh.data.pool;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.DSYNC;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

import cn.ttplatform.wh.GlobalContext;
import cn.ttplatform.wh.config.ServerProperties;
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

/**
 * @author Wang Hao
 * @date 2021/6/8 10:03
 */
@Slf4j
public class BlockCache {

    int dataOffset;
    long fileSize;
    GlobalContext context;
    int blockSize;
    LRU<Long, Block> blocks;
    FlushStrategy flushStrategy;
    ByteBuffer header;
    private final FileChannel fileChannel;

    public BlockCache(GlobalContext context, File file, int dataOffset) {
        this.context = context;
        ServerProperties properties = context.getProperties();
        this.blocks = new LRU<>(properties.getBlockCacheSize());
        this.blockSize = properties.getBlockSize();
        try {
            this.fileChannel = FileChannel.open(file.toPath(), READ, WRITE, CREATE, DSYNC);
            this.dataOffset = dataOffset;
            this.header = fileChannel.map(MapMode.READ_WRITE, 0, dataOffset);
            header.position(0);
            this.fileSize = Bits.getLong(header);
            this.flushStrategy = new PriorityFlushStrategy(properties.getBlockFlushInterval());
        } catch (IOException e) {
            throw new OperateFileException("open file channel error.", e);
        }
    }

    public void updateFileSize(long fileSize) {
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
        if (blockId > fileSize) {
            throw new IndexOutOfBoundsException(
                String.format("index[%d] is greater than fileSize[%d].", blockId, fileSize));
        }
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
        ByteBuffer byteBuffer = ByteBuffer.allocate(blockSize);
        while (position < fileSize) {
            if (!byteBuffer.hasRemaining()) {
                byteBuffers[index++] = byteBuffer;
                byteBuffer = ByteBuffer.allocate(blockSize);
            }
            while (position < fileSize && byteBuffer.hasRemaining()) {
                Block block = getBlock(position);
                ByteBuffer buffer = block.getByteBuffer();
                byteBuffer.position((int) (position - block.startOffset));
                while (buffer.hasRemaining() && byteBuffer.hasRemaining()) {
                    byteBuffer.put(buffer.get());
                    position++;
                }
            }
        }
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
        fileSize += byteBuffer.capacity();
        updateFileSize(fileSize);
    }

    public void appendByteBuffer(ByteBuffer byteBuffer) {
        int offset = 0;
        byteBuffer.flip();
        while (byteBuffer.hasRemaining()) {
            Block block = getBlock(fileSize);
            block.put((int) (fileSize - block.startOffset), byteBuffer);
            fileSize += byteBuffer.position() - offset;
            offset = byteBuffer.position();
        }
        updateFileSize(fileSize);
    }

    public void appendBytes(byte[] bytes) {
        int offset = 0;
        while (offset < bytes.length) {
            Block block = getBlock(fileSize);
            int length = Math.min(bytes.length - offset, (int) (block.endOffset - fileSize));
            block.put((int) (fileSize - block.startOffset), bytes, offset, length);
            offset += length;
            fileSize += length;
        }
        updateFileSize(fileSize);
    }

    public void appendInt(int v) {
        put(fileSize++, Bits.int3(v));
        put(fileSize++, Bits.int2(v));
        put(fileSize++, Bits.int1(v));
        put(fileSize++, Bits.int0(v));
        updateFileSize(fileSize);
    }

    public void appendLong(long v) {
        put(fileSize++, Bits.long7(v));
        put(fileSize++, Bits.long6(v));
        put(fileSize++, Bits.long5(v));
        put(fileSize++, Bits.long4(v));
        put(fileSize++, Bits.long3(v));
        put(fileSize++, Bits.long2(v));
        put(fileSize++, Bits.long1(v));
        put(fileSize++, Bits.long0(v));
        updateFileSize(fileSize);
    }

    public void put(long position, byte b) {
        Block block = getBlock(position);
        block.put((int) (position - block.startOffset), b);
    }

    public int getInt(long position) {
        return Bits.makeInt(get(position++), get(position++), get(position++), get(position));
    }

    public long getLong(long position) {
        return Bits.makeLong(get(position++), get(position++), get(position++), get(position++), get(position++),
            get(position++),
            get(position++), get(position));
    }

    public byte get(long position) {
        Block block = getBlock(position);
        return block.get((int) (position - block.startOffset));
    }

    public void get(long position, byte[] bytes) {
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
        fileSize = offset;
        updateFileSize(fileSize);
    }

    public long getSize() {
        return fileSize;
    }

    public void close() {
        flushStrategy.flush();
    }

    @ToString
    @EqualsAndHashCode
    public class Block {

        long startOffset;
        long endOffset;
        volatile boolean dirty;
        ByteBuffer byteBuffer;

        public Block(long offset) {
            this.startOffset = offset / blockSize * blockSize;
            this.endOffset = startOffset + blockSize - 1;
            this.byteBuffer = ByteBuffer.allocateDirect(blockSize);
            if (fileSize > startOffset) {
                byteBuffer.limit(Math.min((int) (fileSize - startOffset), blockSize));
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
            byteBuffer.flip();
            try {
                fileChannel.write(byteBuffer, startOffset);
                fileChannel.force(true);
            } catch (IOException e) {
                throw new OperateFileException("write an byte[] data to file[index(" + startOffset + ")] error");
            }
        }

        public void put(int position, ByteBuffer source) {
            byteBuffer.position(position);
            byteBuffer.put(source);
            dirty = true;
        }

        public void put(int position, byte[] src, int offset, int length) {
            byteBuffer.position(position);
            byteBuffer.put(src, offset, length);
            dirty = true;
        }

        public void put(int position, byte b) {
            byteBuffer.position(position);
            byteBuffer.put(b);
            dirty = true;
        }

        public void get(int position, byte[] dst, int offset, int length) {
            byteBuffer.position(position);
            byteBuffer.get(dst, offset, length);
        }

        public byte get(int position) {
            byteBuffer.position(position);
            return byteBuffer.get();
        }

        public ByteBuffer getByteBuffer() {
            return byteBuffer;
        }

        public void dirty(boolean dirty) {
            this.dirty = dirty;
        }

        public boolean dirty() {
            return dirty;
        }

        public long getStartOffset() {
            return startOffset;
        }

    }

}
