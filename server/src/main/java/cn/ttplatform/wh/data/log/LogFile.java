package cn.ttplatform.wh.data.log;

import cn.ttplatform.wh.data.tool.Bits;
import cn.ttplatform.wh.data.tool.ByteBufferWriter;
import cn.ttplatform.wh.data.tool.ReadableAndWriteableFile;
import cn.ttplatform.wh.support.Pool;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;

import static cn.ttplatform.wh.data.DataManager.MAX_CHUNK_SIZE;

/**
 * @author Wang Hao
 * @date 2020/7/1 下午10:12
 */
@Slf4j
public class LogFile implements LogOperation {

    private final ReadableAndWriteableFile file;
    private final Pool<ByteBuffer> byteBufferPool;

    public LogFile(File file, Pool<ByteBuffer> byteBufferPool) {
        this.file = new ByteBufferWriter(file, byteBufferPool);
        this.byteBufferPool = byteBufferPool;
    }

    @Override
    public long append(Log log) {
        long offset = file.size();
        byte[] command = log.getCommand();
        int outLength = LogFile.LOG_ENTRY_HEADER_SIZE + command.length;
        ByteBuffer byteBuffer = byteBufferPool.allocate(outLength);
        try {
            Bits.putInt(log.getIndex(), byteBuffer);
            Bits.putInt(log.getTerm(), byteBuffer);
            Bits.putInt(log.getType(), byteBuffer);
            Bits.putInt(command.length, byteBuffer);
            byteBuffer.put(command);
            file.append(byteBuffer, outLength);
        } finally {
            byteBufferPool.recycle(byteBuffer);
        }
        return offset;
    }

    @Override
    public long[] append(List<Log> logs) {
        long[] offsets = new long[logs.size()];
        long base = file.size();
        long offset = base;
        for (int i = 0; i < logs.size(); i++) {
            offsets[i] = offset;
            offset += (LOG_ENTRY_HEADER_SIZE + logs.get(i).getCommand().length);
        }
        int contentLength = (int) (offset - base);
        ByteBuffer byteBuffer = byteBufferPool.allocate(contentLength);
        try {
            for (Log log : logs) {
                Bits.putInt(log.getIndex(), byteBuffer);
                Bits.putInt(log.getTerm(), byteBuffer);
                Bits.putInt(log.getType(), byteBuffer);
                byte[] command = log.getCommand();
                Bits.putInt(command.length, byteBuffer);
                byteBuffer.put(command);
            }
            file.append(byteBuffer, contentLength);
        } finally {
            byteBufferPool.recycle(byteBuffer);
        }
        return offsets;
    }

    @Override
    public void append(ByteBuffer byteBuffer) {
        file.append(byteBuffer, byteBuffer.position());
    }

    /**
     * read a byte array from file start to end, then transfer to LogEntry
     *
     * @param start start offset
     * @param end   end offset
     * @return an log entry
     */
    @Override
    public Log getLog(long start, long end) {
        long size = file.size();
        if (start < 0 || start >= size || start == end) {
            log.debug("get entry from {} to {}.", start, end);
            return null;
        }
        int readLength;
        if (end < start) {
            readLength = (int) (size - start);
        } else {
            readLength = (int) (end - start);
        }
        ByteBuffer byteBuffer = byteBufferPool.allocate(readLength);
        try {
            file.readBytes(start, byteBuffer, readLength);
            // Convert a byte array to {@link LogEntry}
            // index[0-3]
            int index = Bits.getInt(byteBuffer);
            // term[4-7]
            int term = Bits.getInt(byteBuffer);
            // type[8,11]
            int type = Bits.getInt(byteBuffer);
            // commandLength[12,15]
            // cmd[16,content.length]
            int cmdLength = Bits.getInt(byteBuffer);
            byte[] cmd = null;
            if (cmdLength > 0) {
                cmd = new byte[cmdLength];
                byteBuffer.get(cmd, 0, cmdLength);
            }
            return LogFactory.createEntry(type, term, index, cmd, cmdLength);
        } finally {
            byteBufferPool.recycle(byteBuffer);
        }
    }

    @Override
    public void loadLogsIntoList(long start, long end, List<Log> res) {
        int size = (int) (end - start);
        ByteBuffer byteBuffer = byteBufferPool.allocate(size);
        try {
            file.readBytes(start, byteBuffer, size);
            int offset = 0;
            int index;
            int term;
            int type;
            int cmdLength;
            byte[] cmd;
            while (offset < size) {
                index = Bits.getInt(byteBuffer);
                term = Bits.getInt(byteBuffer);
                type = Bits.getInt(byteBuffer);
                cmdLength = Bits.getInt(byteBuffer);
                if (cmdLength == 0) {
                    cmd = null;
                } else {
                    cmd = new byte[cmdLength];
                    byteBuffer.get(cmd, 0, cmdLength);
                }
                offset += cmdLength + LOG_ENTRY_HEADER_SIZE;
                res.add(LogFactory.createEntry(type, term, index, cmd, cmdLength));
            }
        } finally {
            byteBufferPool.recycle(byteBuffer);
        }
    }

    @Override
    public ByteBuffer[] read() {
        int position = 0;
        int fileSize = (int) file.size();
        int size = fileSize % MAX_CHUNK_SIZE == 0 ? fileSize / MAX_CHUNK_SIZE : fileSize / MAX_CHUNK_SIZE + 1;
        ByteBuffer[] buffers = new ByteBuffer[size];
        int index = 0;
        while (position < fileSize) {
            int readLength = Math.min(fileSize - position, MAX_CHUNK_SIZE);
            ByteBuffer byteBuffer = byteBufferPool.allocate(MAX_CHUNK_SIZE);
            file.readBytes(position, byteBuffer, readLength);
            position += readLength;
            buffers[index++] = byteBuffer;
        }
        return buffers;
    }

    @Override
    public void transferTo(long offset, LogOperation dst) {
        ByteBuffer byteBuffer = byteBufferPool.allocate(MAX_CHUNK_SIZE);
        try {
            long fileSize = file.size();
            Bits.putLong(fileSize - offset, byteBuffer);
            dst.append(byteBuffer);
            byteBuffer.clear();
            int readLength;
            while (offset < fileSize) {
                readLength = Math.min(MAX_CHUNK_SIZE, (int) (fileSize - offset));
                file.readBytes(offset, byteBuffer, readLength);
                offset += readLength;
                dst.append(byteBuffer);
            }
        } finally {
            byteBufferPool.recycle(byteBuffer);
        }

    }

    @Override
    public void removeAfter(long offset) {
        file.truncate(offset < 0 ? 0L : offset);
    }

    @Override
    public void close() {
        file.close();
    }

    @Override
    public long size() {
        return file.size();
    }

    @Override
    public boolean isEmpty() {
        return file.size() == 0L;
    }
}
