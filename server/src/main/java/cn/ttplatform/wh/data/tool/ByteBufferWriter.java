package cn.ttplatform.wh.data.tool;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

import cn.ttplatform.wh.constant.ErrorMessage;
import cn.ttplatform.wh.exception.OperateFileException;
import cn.ttplatform.wh.support.Pool;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;

import lombok.extern.slf4j.Slf4j;

/**
 * @author Wang Hao
 * @date 2021/4/19 13:02
 */
@Slf4j
public class ByteBufferWriter implements ReadableAndWriteableFile {

    private final Path path;
    private final FileChannel fileChannel;
    private final Pool<ByteBuffer> bufferPool;
    private final ByteBuffer header;
    /**
     * Not safe in the case of multi-threaded operations
     */
    private long fileSize;

    public ByteBufferWriter(File file, Pool<ByteBuffer> bufferPool) {
        try {
            this.path = file.toPath();
            this.bufferPool = bufferPool;
            // Requires that every update to the file's content be written synchronously to the underlying storage device.
            this.fileChannel = FileChannel.open(path, READ, WRITE, CREATE);
            log.info("open file[{}].", file);
            this.header = ByteBuffer.allocateDirect(8);
            fileChannel.read(header, 0L);
            header.position(0);
            fileSize = Bits.getLong(header);
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

    @Override
    public void writeInt(long position, int data) {
        ByteBuffer byteBuffer = bufferPool.allocate(Integer.BYTES);
        byteBuffer.putInt(data);
        write(byteBuffer, position);
        fileSize = getActualSize();
    }

    @Override
    public int readInt(long position) {
        if (position < 0 || fileSize - position < Integer.BYTES) {
            throw new OperateFileException(ErrorMessage.READ_FAILED);
        }
        ByteBuffer byteBuffer = bufferPool.allocate(Integer.BYTES);
        byteBuffer.limit(Integer.BYTES);
        int read = readBytes(byteBuffer, position);
        if (read < Integer.BYTES) {
            throw new OperateFileException("read an integer data from file tail");
        }
        byteBuffer.flip();
        int res = byteBuffer.getInt();
        bufferPool.recycle(byteBuffer);
        return res;
    }

    @Override
    public void write(long position, byte[] chunk) {
        if (position < 0) {
            throw new UnsupportedOperationException("position can not be less than 0");
        }
        ByteBuffer byteBuffer = bufferPool.allocate(chunk.length);
        byteBuffer.put(chunk);
        write(byteBuffer, position);
        if (position + chunk.length > fileSize) {
            fileSize = getActualSize();
            updateFileSize(fileSize);
        }
    }

    @Override
    public void writeBytes(long position, ByteBuffer byteBuffer) {
        truncate(position);
        write(byteBuffer, position);
    }

    @Override
    public void append(byte[] chunk, int length) {
        ByteBuffer byteBuffer = bufferPool.allocate(length);
        byteBuffer.put(chunk, 0, length);
        write(byteBuffer, fileSize);
        fileSize += length;
        updateFileSize(fileSize);
    }

    @Override
    public void append(ByteBuffer chunk, int length) {
        chunk.limit(length);
        write(chunk, fileSize);
        fileSize += length;
        updateFileSize(fileSize);
    }

    @Override
    public byte[] readBytes(long position, int size) {
        ByteBuffer byteBuffer = bufferPool.allocate(size);
        readBytes(position, byteBuffer, size);
        byte[] res = new byte[size];
        byteBuffer.get(res, 0, size);
        bufferPool.recycle(byteBuffer);
        return res;
    }

    @Override
    public void readBytes(long position, ByteBuffer byteBuffer, int size) {
        if (position < 0 || fileSize - position < size) {
            throw new OperateFileException("not enough content to read");
        }
        byteBuffer.limit(size);
        int read = readBytes(byteBuffer, position);
        if (read != size) {
            log.warn("required {} bytes, but read {} bytes from file[index {}]", size, read, position);
            throw new OperateFileException(
                    String.format("required %d bytes, but read %d bytes from file[index = %d]", size, read, position));
        }
        byteBuffer.flip();
    }

    @Override
    public void transferTo(long position, long count, FileChannel channel) {
        try {
            log.debug("transfer {} bytes from position[{}].", count, position);
            fileChannel.transferTo(position, count, channel);
        } catch (IOException e) {
            throw new OperateFileException(String.format("transfer %d bytes error.", count), e);
        }
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
            updateFileSize(fileSize);
        } catch (IOException e) {
            throw new OperateFileException(String.format("truncate a file[index=%d] error", position), e);
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

    private int readBytes(ByteBuffer byteBuffer, long position) {
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
    public void delete() {
        try {
            Files.delete(path);
            log.info("delete path {}.", path);
        } catch (IOException e) {
            throw new OperateFileException("delete file error.", e);
        }
    }

    @Override
    public long size() {
        return fileSize;
    }

    @Override
    public void close() {
        try {
            if (fileChannel.isOpen()) {
                flush();
                fileChannel.close();
            }
        } catch (IOException e) {
            throw new OperateFileException("close a file channel error");
        }
    }

    @Override
    public void flush() {
        try {
            fileChannel.force(true);
        } catch (IOException e) {
            log.error("failed to flush changes into disk.");
        }
    }

}
