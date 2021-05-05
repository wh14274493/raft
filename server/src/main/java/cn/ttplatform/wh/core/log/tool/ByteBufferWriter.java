package cn.ttplatform.wh.core.log.tool;

import cn.ttplatform.wh.constant.ErrorMessage;
import cn.ttplatform.wh.exception.OperateFileException;
import cn.ttplatform.wh.support.Pool;
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

    private final FileChannel fileChannel;
    private final Pool<ByteBuffer> bufferPool;
    private final Pool<byte[]> byteArrayPool;
    /**
     * Not safe in the case of multi-threaded operations
     */
    private long fileSize;

    public ByteBufferWriter(File file, Pool<ByteBuffer> bufferPool, Pool<byte[]> byteArrayPool) {
        try {
            this.bufferPool = bufferPool;
            this.byteArrayPool = byteArrayPool;
            this.fileChannel = FileChannel
                .open(file.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE,
                    StandardOpenOption.DSYNC);
            log.info("open file[{}].", file);
            fileSize = fileChannel.size();
        } catch (IOException e) {
            throw new OperateFileException("open file channel error.");
        }
    }

    @Override
    public void writeIntAt(long position, int data) {
        ByteBuffer byteBuffer = bufferPool.allocate(Integer.BYTES);
        byteBuffer.putInt(data);
        write(byteBuffer, position);
        fileSize = getActualSize();
    }

    @Override
    public int readIntAt(long position) {
        if (position < 0 || fileSize - position < Integer.BYTES) {
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
    public void writeBytesAt(long position, byte[] chunk) {
        if (position < 0) {
            throw new UnsupportedOperationException("position can not be less than 0");
        }
        ByteBuffer byteBuffer = bufferPool.allocate(chunk.length);
        byteBuffer.put(chunk);
        write(byteBuffer, position);
        fileSize = getActualSize();
    }

    @Override
    public void append(byte[] chunk, int length) {
        ByteBuffer byteBuffer = bufferPool.allocate(chunk.length);
        byteBuffer.put(chunk, 0, length);
        write(byteBuffer, fileSize);
        fileSize += chunk.length;
    }

    @Override
    public byte[] readBytesAt(long position, int size) {
        if (position < 0 || fileSize - position < size) {
            throw new OperateFileException("not enough content to read");
        }
        ByteBuffer byteBuffer = bufferPool.allocate(size);
        int read = read(byteBuffer, position);
        if (read != size) {
            log.warn("required {} bytes, but read {} bytes from file[index {}]", size, read, position);
            throw new OperateFileException(
                "required " + size + " bytes, but read " + read + " bytes from file[index " + position + "]");
        }
        byte[] res = byteArrayPool.allocate(size);
        byteBuffer.flip();
        byteBuffer.get(res, 0, read);
        bufferPool.recycle(byteBuffer);
        return res;
    }

    @Override
    public void truncate(long position) {
        if (position > fileSize) {
            throw new UnsupportedOperationException("position can not be greater than file size");
        }
        try {
            position = Math.max(0L, position);
            fileChannel.truncate(position);
            fileSize = position;
        } catch (IOException e) {
            throw new OperateFileException("truncate a file[index(" + position + ")] error");
        }
    }


    private void write(ByteBuffer byteBuffer, long position) {
        byteBuffer.flip();
        try {
            fileChannel.write(byteBuffer, position);
        } catch (IOException e) {
            throw new OperateFileException("write an byte[] data to file[index(" + position + ")] error");
        } finally {
            bufferPool.recycle(byteBuffer);
        }
    }

    private int read(ByteBuffer byteBuffer, long position) {
        try {
            return fileChannel.read(byteBuffer, position);
        } catch (IOException e) {
            throw new OperateFileException("read an byte[] data to file[index(" + position + ")] error");
        }
    }

    private long getActualSize() {
        try {
            return fileChannel.size();
        } catch (IOException e) {
            throw new OperateFileException("get file size error.");
        }
    }

    @Override
    public void clear() {
        truncate(0L);
    }

    @Override
    public boolean isEmpty() {
        return fileSize == 0L;
    }

    @Override
    public long size() {
        return fileSize;
    }

    @Override
    public void close() {
        try {
            if (fileChannel.isOpen()) {
                fileChannel.close();
            }
        } catch (IOException e) {
            throw new OperateFileException("close a file channel error");
        }
    }

}
