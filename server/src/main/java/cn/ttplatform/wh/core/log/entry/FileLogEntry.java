package cn.ttplatform.wh.core.log.entry;

import cn.ttplatform.wh.core.log.generation.FileName;
import cn.ttplatform.wh.core.log.tool.ByteBufferWriter;
import cn.ttplatform.wh.core.log.tool.PooledByteBuffer;
import cn.ttplatform.wh.core.log.tool.ReadableAndWriteableFile;
import cn.ttplatform.wh.support.Pool;
import java.io.File;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Wang Hao
 * @date 2020/7/1 下午10:12
 */
@Slf4j
public class FileLogEntry {

    /**
     * index(4 bytes) + term(4 bytes) + type(4 bytes) + commandLength(4 bytes) = 16
     */
    public static final int LOG_ENTRY_HEADER_SIZE = 4 + 4 + 4 + 4;
    private final ReadableAndWriteableFile file;
    private final Pool<PooledByteBuffer> byteBufferPool;
    private final Pool<byte[]> byteArrayPool;

    public FileLogEntry(File parent, Pool<PooledByteBuffer> byteBufferPool, Pool<byte[]> byteArrayPool) {
        this.file = new ByteBufferWriter(new File(parent, FileName.LOG_ENTRY_FILE_NAME), byteBufferPool, byteArrayPool);
        this.byteBufferPool = byteBufferPool;
        this.byteArrayPool = byteArrayPool;
    }

    public long append(LogEntry logEntry) {
        long offset = file.size();
        byte[] command = logEntry.getCommand();
        int outLength = FileLogEntry.LOG_ENTRY_HEADER_SIZE + command.length;
        PooledByteBuffer byteBuffer = byteBufferPool.allocate(outLength);
        try {
            byteBuffer.putInt(logEntry.getIndex());
            byteBuffer.putInt(logEntry.getTerm());
            byteBuffer.putInt(logEntry.getType());
            byteBuffer.putInt(command.length);
            byteBuffer.put(command);
            file.append(byteBuffer, outLength);
        } finally {
            byteBufferPool.recycle(byteBuffer);
        }
        return offset;
    }

    public long[] append(List<LogEntry> logEntries) {
        long[] offsets = new long[logEntries.size()];
        long base = file.size();
        long offset = base;
        for (int i = 0; i < logEntries.size(); i++) {
            offsets[i] = offset;
            offset += (LOG_ENTRY_HEADER_SIZE + logEntries.get(i).getCommand().length);
        }
        int contentLength = (int) (offset - base);
        PooledByteBuffer byteBuffer = byteBufferPool.allocate(contentLength);
        try {
            for (LogEntry logEntry : logEntries) {
                byteBuffer.putInt(logEntry.getIndex());
                byteBuffer.putInt(logEntry.getTerm());
                byteBuffer.putInt(logEntry.getType());
                byte[] command = logEntry.getCommand();
                byteBuffer.putInt(command.length);
                byteBuffer.put(command);
            }
            file.append(byteBuffer, contentLength);
        } finally {
            byteBufferPool.recycle(byteBuffer);
        }
        return offsets;
    }

    /**
     * read a byte array from file start to end, then transfer to LogEntry
     *
     * @param start start offset
     * @param end   end offset
     * @return an log entry
     */
    public LogEntry getEntry(long start, long end) {
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
        PooledByteBuffer byteBuffer = null;
        try {
            byteBuffer = file.readByteBufferAt(start, readLength);
            byteBuffer.flip();
            // Convert a byte array to {@link LogEntry}
            // index[0-3]
            int index = byteBuffer.getInt();
            // term[4-7]
            int term = byteBuffer.getInt();
            // type[8,11]
            int type = byteBuffer.getInt();
            // commandLength[12,15]
            // cmd[16,content.length]
            int contentLength = byteBuffer.limit() - LOG_ENTRY_HEADER_SIZE;
            byte[] cmd = byteArrayPool.allocate(contentLength);
            byteBuffer.get(cmd, 0, contentLength);
            return LogEntryFactory.createEntry(type, term, index, cmd, contentLength);
        } finally {
            byteBufferPool.recycle(byteBuffer);
        }

    }

    public void loadEntriesIntoList(long start, long end, List<LogEntry> res) {
        PooledByteBuffer byteBuffer = null;
        try {
            byteBuffer = file.readByteBufferAt(start, (int) (end - start));
            byteBuffer.flip();
            int size = (int) (end - start);
            int offset = 0;
            int index;
            int term;
            int type;
            int cmdLength;
            byte[] cmd;
            while (offset < size) {
                index = byteBuffer.getInt();
                term = byteBuffer.getInt();
                type = byteBuffer.getInt();
                cmdLength = byteBuffer.getInt();
                cmd = new byte[cmdLength];
                byteBuffer.get(cmd, 0, cmdLength);
                offset += cmdLength + LOG_ENTRY_HEADER_SIZE;
                res.add(LogEntryFactory.createEntry(type, term, index, cmd, cmdLength));
            }
        } finally {
            byteBufferPool.recycle(byteBuffer);
        }
    }

    public void removeAfter(long offset) {
        file.truncate(offset < 0 ? 0L : offset);
    }

    public void close() {
        file.close();
    }

    public long size() {
        return file.size();
    }

    public boolean isEmpty() {
        return file.size() == 0L;
    }
}
