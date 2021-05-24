package cn.ttplatform.wh.core.data.tool;

import java.nio.channels.FileChannel;

/**
 * @author Wang Hao
 * @date 2021/4/19 15:10
 */
public interface ReadableAndWriteableFile {

    void writeIntAt(long position, int data);

    int readIntAt(long position);

    void writeBytesAt(long position, byte[] chunk);

    void writeBytesAt(long position, PooledByteBuffer byteBuffer);

    void append(byte[] chunk, int length);

    void append(PooledByteBuffer chunk, int length);

    byte[] readBytesAt(long position, int size);

    PooledByteBuffer readByteBufferAt(long position, int size);

    void transferTo(long position, long count, FileChannel channel);

    long size();

    void truncate(long position);

    void clear();

    boolean isEmpty();

    void delete();

    void close();
}
