package cn.ttplatform.wh.data.support;

import cn.ttplatform.wh.exception.OperateFileException;
import cn.ttplatform.wh.support.Pool;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static cn.ttplatform.wh.constant.ErrorMessage.READ_FAILED;
import static java.nio.file.StandardOpenOption.*;

/**
 * @author Wang Hao
 * @date 2021/6/28 12:45
 */
@Slf4j
public class SyncFileOperator {

    private final FileChannel fileChannel;
    private final Pool<ByteBuffer> bufferPool;
    private FileHeaderOperator headerOperator;
    private long fileSize;

    public SyncFileOperator(File file, Pool<ByteBuffer> bufferPool, FileHeaderOperator headerOperator) {
        try {
            this.bufferPool = bufferPool;
            // Requires that every update to the file's content be written synchronously to the underlying storage device.
            this.fileChannel = FileChannel.open(file.toPath(), READ, WRITE, CREATE);
            this.headerOperator = headerOperator;
            this.fileSize = headerOperator.getFileSize();
            log.info("open the file[{}], and the file size is {}.", file, fileSize);
        } catch (IOException e) {
            throw new OperateFileException("failed to open file channel.", e);
        }
    }

    public void changeFileHeaderOperator(FileHeaderOperator headerOperator) {
        this.headerOperator = headerOperator;
        this.fileSize = headerOperator.getFileSize();
    }

    public void updateFileSize(int increment) {
        fileSize += increment;
        headerOperator.recordFileSize(fileSize);
    }

    public void append(byte[] bytes, int length) {
        ByteBuffer byteBuffer = bufferPool.allocate(bytes.length);
        byteBuffer.put(bytes, 0, length);
        byteBuffer.flip();
        try {
            fileChannel.write(byteBuffer, fileSize);
        } catch (IOException e) {
            throw new OperateFileException(String.format("failed to write %d bytes into file at position[%d].", length, fileSize), e);
        } finally {
            bufferPool.recycle(byteBuffer);
        }
        updateFileSize(length);
    }

    public void append(ByteBuffer byteBuffer, int length) {
        byteBuffer.limit(length);
        byteBuffer.position(0);
        try {
            fileChannel.write(byteBuffer, fileSize);
        } catch (IOException e) {
            throw new OperateFileException(String.format("failed to write %d bytes into file at position[%d].", length, fileSize), e);
        }
        updateFileSize(length);
    }

    public byte[] readBytes(long position, int length) {
        if (position < 0 || fileSize - position < length) {
            throw new OperateFileException(READ_FAILED);
        }
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
        if (position < 0 || fileSize - position < length) {
            throw new OperateFileException(READ_FAILED);
        }
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
        if (position > fileSize) {
            log.warn("truncate position can not be greater than file size.");
            return;
        }
        try {
            position = Math.max(0, position);
            fileChannel.truncate(position);
            updateFileSize((int) (position - fileSize));
        } catch (IOException e) {
            throw new OperateFileException(String.format("failed to truncate a file at position[%d].", position), e);
        }
    }

    public boolean isEmpty() {
        return fileSize <= 0;
    }

    public long size() {
        return fileSize;
    }

    public void close() {
        try {
            headerOperator.force();
            if (fileChannel.isOpen()) {
                fileChannel.force(true);
                fileChannel.close();
            }
        } catch (IOException e) {
            throw new OperateFileException("failed to close a file channel.", e);
        }
    }

}
