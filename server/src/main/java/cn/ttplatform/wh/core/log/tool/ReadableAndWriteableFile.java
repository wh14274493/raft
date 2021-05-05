package cn.ttplatform.wh.core.log.tool;

/**
 * @author Wang Hao
 * @date 2021/4/19 15:10
 */
public interface ReadableAndWriteableFile {

    void writeIntAt(long position, int data);

    int readIntAt(long position);

    void writeBytesAt(long position, byte[] chunk);

    void append(byte[] chunk,int length);

    byte[] readBytesAt(long position, int size);

    long size();

    void truncate(long position);

    void clear();

    boolean isEmpty();

    void close();
}
