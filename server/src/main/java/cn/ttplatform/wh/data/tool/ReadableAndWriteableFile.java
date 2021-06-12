package cn.ttplatform.wh.data.tool;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author Wang Hao
 * @date 2021/4/19 15:10
 */
public interface ReadableAndWriteableFile {

    void writeInt(long position, int data);

    int readInt(long position);

    void write(long position, byte[] chunk);

    void writeBytes(long position, ByteBuffer byteBuffer);

    void append(byte[] chunk, int length);

    void append(ByteBuffer chunk, int length);

    byte[] readBytes(long position, int size);

    void readBytes(long position, ByteBuffer byteBuffer, int size);

    void transferTo(long position, long count, FileChannel channel);

    long size();

    void truncate(long position);

    void clear();

    boolean isEmpty();

    void delete();

    void close();

    void flush();
}
