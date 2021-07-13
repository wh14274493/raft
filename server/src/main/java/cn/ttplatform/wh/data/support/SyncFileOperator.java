package cn.ttplatform.wh.data.support;

import cn.ttplatform.wh.exception.OperateFileException;
import cn.ttplatform.wh.support.Pool;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static java.nio.file.StandardOpenOption.*;

/**
 * @author Wang Hao
 * @date 2021/6/28 12:45
 */
@Slf4j
public class SyncFileOperator {

    private final FileChannel fileChannel;
    private final Pool<ByteBuffer> bufferPool;

    public SyncFileOperator(File file, Pool<ByteBuffer> bufferPool) {
        try {
            this.bufferPool = bufferPool;
            // Requires that every update to the file's content be written synchronously to the underlying storage device.
            this.fileChannel = FileChannel.open(file.toPath(), READ, WRITE, CREATE);
        } catch (IOException e) {
            throw new OperateFileException("failed to open file channel.", e);
        }
    }

    public void append(long position, byte[] bytes, int length) {
        ByteBuffer byteBuffer = bufferPool.allocate(bytes.length);
        byteBuffer.put(bytes, 0, length);
        byteBuffer.flip();
        try {
            fileChannel.write(byteBuffer, position);
        } catch (IOException e) {
            throw new OperateFileException(String.format("failed to write %d bytes into file at position[%d].", length, position), e);
        } finally {
            bufferPool.recycle(byteBuffer);
        }
    }

    public void append(long position, ByteBuffer byteBuffer, int length) {
        byteBuffer.limit(length);
        byteBuffer.position(0);
        try {
            fileChannel.write(byteBuffer, position);
        } catch (IOException e) {
            throw new OperateFileException(String.format("failed to write %d bytes into file at position[%d].", length, position), e);
        }
    }

    public byte[] readBytes(long position, int length) {
        ByteBuffer byteBuffer = bufferPool.allocate(length);
        try {
            readBytes(position, byteBuffer, length);
            byte[] res = new byte[length];
            byteBuffer.get(res, 0, length);
            return res;
        } finally {
            bufferPool.recycle(byteBuffer);
        }
    }

    public void readBytes(long position, ByteBuffer byteBuffer, int length) {
        byteBuffer.limit(length);
        int read;
        try {
            read = fileChannel.read(byteBuffer, position);
        } catch (IOException e) {
            throw new OperateFileException(String.format("failed to read %d bytes from file at position[%d].", byteBuffer.limit(), position), e);
        }
        if (read != length) {
            throw new OperateFileException(String.format("required %d bytes, but read %d bytes from file at position[%d]", length, read, position));
        }
        byteBuffer.flip();
    }

    public void truncate(long position) {
        try {
            position = Math.max(0, position);
            fileChannel.truncate(position);
        } catch (IOException e) {
            throw new OperateFileException(String.format("failed to truncate a file at position[%d].", position), e);
        }
    }

    public void close() {
        try {
            if (fileChannel.isOpen()) {
                fileChannel.force(true);
                fileChannel.close();
            }
        } catch (IOException e) {
            throw new OperateFileException("failed to close a file channel.", e);
        }
    }

}
