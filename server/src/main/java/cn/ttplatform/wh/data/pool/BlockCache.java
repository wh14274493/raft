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
            this.header = ByteBuffer.allocateDirect(dataOffset);
            if (fileChannel.size() > dataOffset) {
                fileChannel.read(header);
            }
            header.flip();
            this.fileSize = Bits.getLong(header);
            this.flushStrategy = new PriorityFlushStrategy();
        } catch (IOException e) {
            throw new OperateFileException("open file channel error.", e);
        }
    }

    public void updateFileSize(long fileSize) {
        header.clear();
        Bits.putLong(fileSize, header);
        try {
            fileChannel.write(header);
        } catch (IOException e) {
            throw new OperateFileException("write file size error.", e);
        }
    }

    public void readFileSize() {
        try {
            fileChannel.read(header);
        } catch (IOException e) {
            throw new OperateFileException("read file size error.", e);
        }
        header.flip();
        this.fileSize = Bits.getLong(header);
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
            throw new IndexOutOfBoundsException(String.format("index[%d] is greater than fileSize[%d].", blockId, fileSize));
        }
        Block block = blocks.get(blockId);
        if (block == null) {
            block = new Block(blockId);
            addBlock(blockId, block);
        }
        return block;
    }

    public void addBlock(long blockId, Block block) {
        KVEntry<Long, Block> kvEntry = blocks.put(blockId, block);
        if (kvEntry != null) {
            Block removed = kvEntry.getValue();
            if (removed.dirty()) {
                flushStrategy.flush(removed);
            }
        }
    }

    public void appendByteBuffer(ByteBuffer byteBuffer) {
        addBlock(fileSize, new Block(fileSize, byteBuffer));
        fileSize += byteBuffer.position();
        updateFileSize(fileSize);
    }

    public void appendBytes(byte[] bytes) {
        put(fileSize, bytes);
        fileSize += bytes.length;
        updateFileSize(fileSize);
    }

    public void appendInt(int v) {
        putInt(fileSize, v);
        fileSize += 4;
        updateFileSize(fileSize);
    }

    public void putInt(long position, int value) {
        put(position++, Bits.int3(value));
        put(position++, Bits.int2(value));
        put(position++, Bits.int1(value));
        put(position, Bits.int0(value));
    }

    public int getInt(long position) {
        return Bits.makeInt(get(position++), get(position++), get(position++), get(position));
    }

    public void put(long position, byte b) {
        Block block = getBlock(position);
        ByteBuffer byteBuffer = block.getByteBuffer();
        byteBuffer.position((int) (position - block.startOffset));
        byteBuffer.put(b);
        block.dirty(true);
    }

    public byte get(long position) {
        Block block = getBlock(position);
        ByteBuffer byteBuffer = block.getByteBuffer();
        byteBuffer.position((int) (position - block.startOffset));
        return byteBuffer.get();
    }

    public void put(long position, byte[] bytes) {
        fileSize = Math.max(fileSize, position + bytes.length);
        int offset = 0;
        while (offset < bytes.length) {
            Block block = getBlock(position);
            block.dirty(true);
            ByteBuffer byteBuffer = block.getByteBuffer();
            byteBuffer.position((int) (position - block.startOffset));
            int length = Math.min(bytes.length - offset, (int) (block.endOffset - position));
            byteBuffer.put(bytes, offset, length);
            offset += length;
            position += length;
        }
    }

    public void get(long position, byte[] bytes) {
        int offset = 0;
        while (offset < bytes.length && position < fileSize) {
            Block block = getBlock(position);
            ByteBuffer byteBuffer = block.getByteBuffer();
            byteBuffer.position((int) (position - block.startOffset));
            int length = Math.min(bytes.length - offset, (int) (block.endOffset - position));
            byteBuffer.get(bytes, offset, length);
            offset += length;
            position += length;
        }
    }

    public void get(long position, ByteBuffer byteBuffer) {
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

    public ByteBuffer[] getBuffers(long position) {
        ByteBuffer[] byteBuffers = new ByteBuffer[(int) ((fileSize - position) / blockSize) + 1];
        int index = 0;
        ByteBuffer byteBuffer = ByteBuffer.allocate(blockSize);
        while (position < fileSize) {
            if (!byteBuffer.hasRemaining()) {
                byteBuffers[index++] = byteBuffer;
                byteBuffer = ByteBuffer.allocate(blockSize);
            }
            get(position, byteBuffer);
        }
        return byteBuffers;
    }


    public void removeAfter(long offset) {
        long blockId = calculateBlockId(offset);
        // remove all block that startOffset > offset in cache.
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
