package cn.ttplatform.wh.data.support;

import cn.ttplatform.wh.exception.OperateFileException;
import cn.ttplatform.wh.support.Pool;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import static cn.ttplatform.wh.constant.ErrorMessage.INDEX_OUT_OF_BOUND;
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
    private final FileHeaderOperator headerOperator;
    private final FileBodyOperator bodyOperator;
    private final long dataOffset;
    private long fileSize;

    private class FileHeaderOperator {

        private final MappedByteBuffer header;
        private final long endOffset;

        public FileHeaderOperator(int dataOffset) throws IOException {
            this.endOffset = dataOffset;
            this.header = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, endOffset);
        }

        public void clear() {
            header.position(0);
            Bits.putLong(endOffset, header);
            while (header.hasRemaining()) {
                header.put((byte) 0);
            }
        }

        public void writeInt(long position, int v) {
            if (position < 0 || position > endOffset - Integer.BYTES) {
                throw new IllegalArgumentException(String.format(INDEX_OUT_OF_BOUND, position, 0, endOffset - Integer.BYTES));
            }
            header.position((int) position);
            Bits.putInt(v, header);
        }

        public void writeLong(long position, long v) {
            if (position < 0 || position > endOffset - Long.BYTES) {
                throw new IllegalArgumentException(String.format(INDEX_OUT_OF_BOUND, position, 0, endOffset - Long.BYTES));
            }
            header.position((int) position);
            Bits.putLong(v, header);
        }

        public int readInt(long position) {
            if (position < 0 || position > endOffset - Integer.BYTES) {
                throw new IllegalArgumentException(String.format(INDEX_OUT_OF_BOUND, position, 0, endOffset - Integer.BYTES));
            }
            header.position((int) position);
            return Bits.getInt(header);
        }

        public long readLong(long position) {
            if (position < 0 || position > endOffset - Long.BYTES) {
                throw new IllegalArgumentException(String.format(INDEX_OUT_OF_BOUND, position, 0, endOffset - Long.BYTES));
            }
            header.position((int) position);
            return Bits.getLong(header);
        }

        public void force() {
            header.force();
        }
    }

    private class FileBodyOperator {

        private final long startOffset;

        public FileBodyOperator(long startOffset) {
            this.startOffset = startOffset;
        }

        public void writeBytes(long position, byte[] bytes, int length) {
            if (position >= startOffset) {
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
        }

        public void writeBytes(long position, ByteBuffer byteBuffer, int length) {
            if (position < startOffset) {
                throw new OperateFileException(String.format("position should be greater than %d", startOffset - 1));
            }
            byteBuffer.limit(length);
            byteBuffer.position(0);
            try {
                fileChannel.write(byteBuffer, position);
            } catch (IOException e) {
                throw new OperateFileException(String.format("failed to write %d bytes into file at position[%d].", length, position), e);
            }
        }

        public byte[] readBytes(long position, int length) {
            if (position < startOffset || fileSize - position < length) {
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
            if (position < startOffset || fileSize - position < length) {
                throw new OperateFileException(READ_FAILED);
            }
            byteBuffer.limit(length);
            int read = read(byteBuffer, position);
            if (read != length) {
                throw new OperateFileException(String.format("required %d bytes, but read %d bytes from file at position[%d]", length, read, position));
            }
            byteBuffer.flip();
        }

        private int read(ByteBuffer byteBuffer, long position) {
            try {
                return fileChannel.read(byteBuffer, position);
            } catch (IOException e) {
                throw new OperateFileException(String.format("failed to read %d bytes from file at position[%d].", byteBuffer.limit(), position), e);
            }
        }
    }

    public SyncFileOperator(File file, Pool<ByteBuffer> bufferPool, int dataOffset) {
        try {
            this.bufferPool = bufferPool;
            // Requires that every update to the file's content be written synchronously to the underlying storage device.
            this.fileChannel = FileChannel.open(file.toPath(), READ, WRITE, CREATE, DSYNC);
            this.headerOperator = new FileHeaderOperator(dataOffset);
            this.bodyOperator = new FileBodyOperator(dataOffset);
            this.fileSize = fileChannel.size();
            if (fileSize < dataOffset) {
                updateFileSize((int) (dataOffset - fileSize));
            }
            this.dataOffset = dataOffset;
            log.info("open file[{}].", file);
        } catch (IOException e) {
            throw new OperateFileException("failed to open file channel.", e);
        }
    }

    public void updateFileSize(int increment) {
        fileSize += increment;
        if (fileSize == dataOffset) {
            headerOperator.clear();
        } else {
            headerOperator.writeLong(0, fileSize);
        }
    }

    public void writeHeader(long position, int v) {
        headerOperator.writeInt(position, v);
    }

    public void writeHeader(long position, long v) {
        headerOperator.writeLong(position, v);
    }

    public int readIntFromHeader(long position) {
        return headerOperator.readInt(position);
    }

    public long readLongFromHeader(long position) {
        return headerOperator.readLong(position);
    }

    public void append(byte[] bytes, int length) {
        bodyOperator.writeBytes(fileSize, bytes, length);
        updateFileSize(length);
    }

    public void append(ByteBuffer byteBuffer, int length) {
        bodyOperator.writeBytes(fileSize, byteBuffer, length);
        updateFileSize(length);
    }

    public byte[] readBytes(long position, int length) {
        if (position < 0 || fileSize - position < length) {
            throw new OperateFileException(READ_FAILED);
        }
        return bodyOperator.readBytes(position, length);
    }

    public void readBytes(long position, ByteBuffer byteBuffer, int length) {
        if (position < 0 || fileSize - position < length) {
            throw new OperateFileException(READ_FAILED);
        }
        bodyOperator.readBytes(position, byteBuffer, length);
    }

    public void truncate(long position) {
        if (position > fileSize) {
            log.warn("truncate position can not be greater than file size.");
            return;
        }
        try {
            position = Math.max(dataOffset, position);
            fileChannel.truncate(position);
            updateFileSize((int) (position - fileSize));
        } catch (IOException e) {
            throw new OperateFileException(String.format("failed to truncate a file at position[%d].", position), e);
        }
    }

    public boolean isEmpty() {
        return fileSize <= dataOffset;
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
