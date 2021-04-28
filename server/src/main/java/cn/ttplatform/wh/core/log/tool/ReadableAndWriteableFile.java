package cn.ttplatform.wh.core.log.tool;

/**
 * @author Wang Hao
 * @date 2021/4/19 15:10
 */
public interface ReadableAndWriteableFile {

    void seek(long position);

    void writeInt(int data);

    void writeIntAt(long position, int data);

    int readInt();

    int readIntAt(long position);

    void writeLong(long data);

    void writeLongAt(long position, long data);

    long readLong();

    long readLongAt(long position);

    void writeBytes(byte[] data);

    void writeBytesAt(long position, byte[] data);

    void append(byte[] data);

    byte[] readBytes(int size);

    byte[] readBytesAt(long position, int size);

    long size();

    void truncate(long position);

    void clear();

    boolean isEmpty();

    void close();
}
