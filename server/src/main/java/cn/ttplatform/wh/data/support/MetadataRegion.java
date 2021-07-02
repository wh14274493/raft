package cn.ttplatform.wh.data.support;

import cn.ttplatform.wh.exception.OperateFileException;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import static cn.ttplatform.wh.constant.ErrorMessage.INDEX_OUT_OF_BOUND;
import static java.nio.file.StandardOpenOption.*;

/**
 * @author Wang Hao
 * @date 2021/7/1 13:57
 */
public class MetadataRegion {

    private final MappedByteBuffer region;
    /**
     * included in mapped region
     */
    private final long left;
    /**
     * excluded in mapped region
     */
    private final long right;

    public MetadataRegion(File metaFile, long position, long size) {
        this.left = 0;
        this.right = size;
        try (FileChannel fileChannel = FileChannel.open(metaFile.toPath(), READ, WRITE, CREATE)) {
            this.region = fileChannel.map(FileChannel.MapMode.READ_WRITE, position, size);
        } catch (IOException e) {
            throw new OperateFileException(String.format("failed to map a memory region[%d,%d].", position, position + size - 1), e);
        }
    }

    public void clear() {
        region.position(0);
        while (region.hasRemaining()) {
            region.put((byte) 0);
        }
    }

    public void writeInt(long position, int v) {
        if (position < left || position > right - Integer.BYTES) {
            throw new IllegalArgumentException(String.format(INDEX_OUT_OF_BOUND, position, left, right - Integer.BYTES));
        }
        region.putInt((int) position, v);
    }

    public void writeLong(long position, long v) {
        if (position < left || position > right - Long.BYTES) {
            throw new IllegalArgumentException(String.format(INDEX_OUT_OF_BOUND, position, 0, right - Long.BYTES));
        }
        region.putLong((int) position, v);
    }

    public int readInt(long position) {
        if (position < left || position > right - Integer.BYTES) {
            throw new IllegalArgumentException(String.format(INDEX_OUT_OF_BOUND, position, 0, right - Integer.BYTES));
        }
        return region.getInt((int) position);
    }

    public long readLong(long position) {
        if (position < left || position > right - Long.BYTES) {
            throw new IllegalArgumentException(String.format(INDEX_OUT_OF_BOUND, position, 0, right - Long.BYTES));
        }
        return region.getLong((int) position);
    }

    public ByteBuffer read() {
        return region;
    }

    public void write(ByteBuffer byteBuffer) {
        byteBuffer.position(0);
        region.position(0);
        region.put(byteBuffer);
    }

    public void force() {
        region.force();
    }
}
