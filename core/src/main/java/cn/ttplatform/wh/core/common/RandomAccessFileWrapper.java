package cn.ttplatform.wh.core.common;

import cn.ttplatform.wh.constant.ExceptionMessage;
import cn.ttplatform.wh.exception.OperateFileException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * @author : wang hao
 * @date :  2020/8/15 22:50
 **/
public class RandomAccessFileWrapper {

    private final RandomAccessFile randomAccessFile;

    public RandomAccessFileWrapper(File file) {
        try {
            if (!file.exists() && !file.createNewFile()) {
                throw new OperateFileException(ExceptionMessage.CREATE_FILE_ERROR);
            }
            randomAccessFile = new RandomAccessFile(file, "rw");
        } catch (IOException e) {
            throw new OperateFileException(ExceptionMessage.CREATE_FILE_ERROR, e.getCause());
        }
    }

    public void seek(long position) {
        try {
            randomAccessFile.seek(position);
        } catch (IOException e) {
            throw new OperateFileException(ExceptionMessage.SEEK_POSITION_ERROR, e.getCause());
        }
    }

    public void writeInt(int data) {
        try {
            randomAccessFile.writeInt(data);
        } catch (IOException e) {
            throw new OperateFileException(ExceptionMessage.WRITE_FILE_ERROR, e.getCause());
        }
    }

    public void writeIntAt(long position, int data) {
        seek(position);
        writeInt(data);
    }

    public int readInt() {
        try {
            return randomAccessFile.readInt();
        } catch (IOException e) {
            throw new OperateFileException(ExceptionMessage.READ_FILE_ERROR, e.getCause());
        }
    }

    public int readIntAt(long position) {
        seek(position);
        return readInt();
    }

    public void writeLong(long data) {
        try {
            randomAccessFile.writeLong(data);
        } catch (IOException e) {
            throw new OperateFileException(ExceptionMessage.WRITE_FILE_ERROR, e.getCause());
        }
    }

    public void writeLongAt(long position, long data) {
        seek(position);
        writeLong(data);
    }

    public long readLong() {
        try {
            return randomAccessFile.readLong();
        } catch (IOException e) {
            throw new OperateFileException(ExceptionMessage.READ_FILE_ERROR, e.getCause());
        }
    }

    public long readLongAt(long position) {
        seek(position);
        return readLong();
    }

    public void writeBytes(byte[] data) {
        try {
            randomAccessFile.write(data);
        } catch (IOException e) {
            throw new OperateFileException(ExceptionMessage.WRITE_FILE_ERROR, e.getCause());
        }
    }

    public void writeBytesAt(long position, byte[] data) {
        seek(position);
        writeBytes(data);
    }

    public void append(byte[] data) {
        writeBytesAt(size(), data);
    }

    public byte[] readBytes(int size) {
        try {
            byte[] content = new byte[size];
            randomAccessFile.read(content);
            return content;
        } catch (IOException e) {
            throw new OperateFileException(ExceptionMessage.READ_FILE_ERROR, e.getCause());
        }
    }

    public byte[] readBytesAt(long position, int size) {
        seek(position);
        return readBytes(size);
    }

    public long size() {
        try {
            return randomAccessFile.length();
        } catch (IOException e) {
            throw new OperateFileException(ExceptionMessage.READ_FILE_LENGTH_ERROR, e.getCause());
        }
    }

    public void truncate(long position) {
        try {
            randomAccessFile.setLength(position);
        } catch (IOException e) {
            throw new OperateFileException(ExceptionMessage.TRUNCATE_FILE_ERROR, e.getCause());
        }
    }

    public void clear() {
        truncate(0L);
    }

    public boolean isEmpty() {
        return size() == 0L;
    }

    public void close() {
        try {
            randomAccessFile.close();
        } catch (IOException e) {
            throw new OperateFileException(ExceptionMessage.CLOSE_FILE_ERROR, e.getCause());
        }
    }
}
