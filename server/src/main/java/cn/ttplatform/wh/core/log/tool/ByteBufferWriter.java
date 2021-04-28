package cn.ttplatform.wh.core.log.tool;

import cn.ttplatform.wh.constant.ErrorMessage;
import cn.ttplatform.wh.exception.OperateFileException;
import cn.ttplatform.wh.support.BufferPool;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Wang Hao
 * @date 2021/4/19 13:02
 */
@Slf4j
public class ByteBufferWriter implements ReadableAndWriteableFile {

    FileChannel channel;
    BufferPool<ByteBuffer> bufferPool;

    public ByteBufferWriter(File file, BufferPool<ByteBuffer> bufferPool) {
        try {
            this.bufferPool = bufferPool;
            channel = FileChannel
                .open(file.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE,
                    StandardOpenOption.DSYNC,StandardOpenOption.DELETE_ON_CLOSE);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void seek(long position) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeInt(int data) {
        writeIntAt(size(), data);
    }

    @Override
    public void writeIntAt(long position, int data) {
        ByteBuffer byteBuffer = bufferPool.allocate(Integer.BYTES);
        byteBuffer.putInt(data);
        write(byteBuffer, position);
    }

    @Override
    public int readInt() {
        long fileSize = size();
        if (fileSize < Integer.BYTES) {
            throw new OperateFileException(ErrorMessage.READ_FAILED);
        }
        return readIntAt(fileSize - Integer.BYTES);
    }

    @Override
    public int readIntAt(long position) {
        if (position < 0 || size() - position < Integer.BYTES) {
            throw new OperateFileException(ErrorMessage.READ_FAILED);
        }
        ByteBuffer byteBuffer = bufferPool.allocate(Integer.BYTES);
        int read = read(byteBuffer, position);
        if (read < Integer.BYTES) {
            throw new OperateFileException("read an integer data from file tail");
        }
        byteBuffer.flip();
        int res = byteBuffer.getInt();
        bufferPool.recycle(byteBuffer);
        return res;
    }

    @Override
    public void writeLong(long data) {
        writeLongAt(size(), data);
    }

    @Override
    public void writeLongAt(long position, long data) {
        if (position < 0) {
            throw new UnsupportedOperationException("position can not be less than 0");
        }
        ByteBuffer byteBuffer = bufferPool.allocate(Long.BYTES);
        byteBuffer.putLong(data);
        write(byteBuffer, position);
    }

    @Override
    public long readLong() {
        long fileSize = size();
        if (fileSize < Long.BYTES) {
            throw new OperateFileException(ErrorMessage.READ_FAILED);
        }
        return readLongAt(fileSize - Long.BYTES);
    }

    @Override
    public long readLongAt(long position) {
        if (position < 0 || size() - position < Long.BYTES) {
            throw new OperateFileException("not enough content to read");
        }
        ByteBuffer byteBuffer = bufferPool.allocate(Long.BYTES);
        int read = read(byteBuffer, position);
        if (read < Long.BYTES) {
            throw new OperateFileException("read an long data from file[index(" + position + ")] tail");
        }
        byteBuffer.flip();
        long res = byteBuffer.getLong();
        bufferPool.recycle(byteBuffer);
        return res;
    }

    @Override
    public void writeBytes(byte[] data) {
        writeBytesAt(size(), data);
    }

    @Override
    public void writeBytesAt(long position, byte[] data) {
        if (position < 0) {
            throw new UnsupportedOperationException("position can not be less than 0");
        }
        ByteBuffer byteBuffer = bufferPool.allocate(data.length);
        byteBuffer.put(data);
        write(byteBuffer, position);
    }

    @Override
    public void append(byte[] data) {
        writeBytes(data);
    }

    @Override
    public byte[] readBytes(int size) {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte[] readBytesAt(long position, int size) {
        if (position < 0 || size() - position < size) {
            throw new OperateFileException("not enough content to read");
        }
        ByteBuffer byteBuffer = bufferPool.allocate(size);
        int read = read(byteBuffer, position);
        if (read < size) {
            log.warn("required {} bytes, but read {} bytes from file[index {}]", size, read, position);
            throw new OperateFileException(
                "required " + size + " bytes, but read " + read + " bytes from file[index " + position + "]");
        }
        byte[] res = new byte[size];
        byteBuffer.flip();
        byteBuffer.get(res);
        bufferPool.recycle(byteBuffer);
        return res;
    }

    @Override
    public void truncate(long position) {
        if (position > size()) {
            throw new UnsupportedOperationException("position can not be greater than file size");
        }
        try {
            channel.truncate(Math.max(0L, position));
        } catch (IOException e) {
            throw new OperateFileException("truncate a file[index(" + position + ")] error");
        }
    }

    @Override
    public void clear() {
        truncate(0L);
    }

    @Override
    public boolean isEmpty() {
        return size() == 0L;
    }

    @Override
    public void close() {
        try {
            if (channel.isOpen()) {
                channel.close();
            }
        } catch (IOException e) {
            throw new OperateFileException("close a file channel error");
        }
    }

    @Override
    public long size() {
        try {
            return channel.size();
        } catch (IOException e) {
            throw new OperateFileException("read file size error");
        }
    }

    private void write(ByteBuffer byteBuffer, long position) {
        byteBuffer.flip();
        try {
            channel.write(byteBuffer, position);
        } catch (IOException e) {
            throw new OperateFileException("write an byte[] data to file[index(" + position + ")] error");
        } finally {
            bufferPool.recycle(byteBuffer);
        }
    }

    private int read(ByteBuffer byteBuffer, long position) {
        try {
            return channel.read(byteBuffer, position);
        } catch (IOException e) {
            throw new OperateFileException("read an byte[] data to file[index(" + position + ")] error");
        }
    }
}
