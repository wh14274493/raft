package cn.ttplatform.wh.core.support;

import cn.ttplatform.wh.constant.ErrorMessage;
import cn.ttplatform.wh.exception.OperateFileException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * @author : wang hao
 * @date :  2020/8/15 22:50
 **/
@Deprecated
public class RandomAccessFileWrapper implements ReadableAndWriteableFile {

    RandomAccessFile randomAccessFile;

    public RandomAccessFileWrapper(File file) {
        try {
            if (!file.exists() && !file.createNewFile()) {
                throw new OperateFileException(ErrorMessage.CREATE_FILE_ERROR);
            }
            randomAccessFile = new RandomAccessFile(file, "rw");
        } catch (IOException e) {
            throw new OperateFileException(ErrorMessage.CREATE_FILE_ERROR, e.getCause());
        }
    }

    @Override
    public void seek(long position) {
        try {
            randomAccessFile.seek(position);
        } catch (IOException e) {
            throw new OperateFileException(ErrorMessage.SEEK_POSITION_ERROR, e.getCause());
        }
    }

    @Override
    public void writeInt(int data) {
        try {
            randomAccessFile.writeInt(data);
        } catch (IOException e) {
            throw new OperateFileException(ErrorMessage.WRITE_FILE_ERROR, e.getCause());
        }
    }

    @Override
    public void writeIntAt(long position, int data) {
        seek(position);
        writeInt(data);
    }

    @Override
    public int readInt() {
        try {
            return randomAccessFile.readInt();
        } catch (IOException e) {
            throw new OperateFileException(ErrorMessage.READ_FILE_ERROR, e.getCause());
        }
    }

    @Override
    public int readIntAt(long position) {
        seek(position);
        return readInt();
    }

    @Override
    public void writeLong(long data) {
        try {
            randomAccessFile.writeLong(data);
        } catch (IOException e) {
            throw new OperateFileException(ErrorMessage.WRITE_FILE_ERROR, e.getCause());
        }
    }

    @Override
    public void writeLongAt(long position, long data) {
        seek(position);
        writeLong(data);
    }

    @Override
    public long readLong() {
        try {
            return randomAccessFile.readLong();
        } catch (IOException e) {
            throw new OperateFileException(ErrorMessage.READ_FILE_ERROR, e.getCause());
        }
    }

    @Override
    public long readLongAt(long position) {
        seek(position);
        return readLong();
    }

    @Override
    public void writeBytes(byte[] data) {
        try {
            randomAccessFile.write(data);
        } catch (IOException e) {
            throw new OperateFileException(ErrorMessage.WRITE_FILE_ERROR, e.getCause());
        }
    }

    @Override
    public void writeBytesAt(long position, byte[] data) {
        seek(position);
        writeBytes(data);
    }

    @Override
    public void append(byte[] data) {
        writeBytesAt(size(), data);
    }

    @Override
    public byte[] readBytes(int size) {
        try {
            byte[] content = new byte[size];
            randomAccessFile.read(content);
            return content;
        } catch (IOException e) {
            throw new OperateFileException(ErrorMessage.READ_FILE_ERROR, e.getCause());
        }
    }

    @Override
    public byte[] readBytesAt(long position, int size) {
        seek(position);
        return readBytes(size);
    }

    @Override
    public long size() {
        try {
            return randomAccessFile.length();
        } catch (IOException e) {
            throw new OperateFileException(ErrorMessage.READ_FILE_LENGTH_ERROR, e.getCause());
        }
    }

    @Override
    public void truncate(long position) {
        try {
            randomAccessFile.setLength(position);
        } catch (IOException e) {
            throw new OperateFileException(ErrorMessage.TRUNCATE_FILE_ERROR, e.getCause());
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
            randomAccessFile.close();
        } catch (IOException e) {
            throw new OperateFileException(ErrorMessage.CLOSE_FILE_ERROR, e.getCause());
        }
    }
}
