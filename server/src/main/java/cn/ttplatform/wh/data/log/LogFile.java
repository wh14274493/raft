package cn.ttplatform.wh.data.log;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.DSYNC;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

import cn.ttplatform.wh.data.tool.ByteBufferWriter;
import cn.ttplatform.wh.data.tool.PooledByteBuffer;
import cn.ttplatform.wh.data.tool.ReadableAndWriteableFile;
import cn.ttplatform.wh.support.Pool;
import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Wang Hao
 * @date 2020/7/1 下午10:12
 */
@Slf4j
public class LogFile {

    /**
     * index(4 bytes) + term(4 bytes) + type(4 bytes) + commandLength(4 bytes) = 16
     */
    public static final int LOG_ENTRY_HEADER_SIZE = 4 + 4 + 4 + 4;
    private final ReadableAndWriteableFile file;
    private final Pool<PooledByteBuffer> byteBufferPool;

    public LogFile(File file, Pool<PooledByteBuffer> byteBufferPool) {
        this.file = new ByteBufferWriter(file, byteBufferPool);
        this.byteBufferPool = byteBufferPool;
    }

    public long append(Log log) {
        long offset = file.size();
        byte[] command = log.getCommand();
        int outLength = LogFile.LOG_ENTRY_HEADER_SIZE + command.length;
        PooledByteBuffer byteBuffer = byteBufferPool.allocate(outLength);
        try {
            byteBuffer.putInt(log.getIndex());
            byteBuffer.putInt(log.getTerm());
            byteBuffer.putInt(log.getType());
            byteBuffer.putInt(command.length);
            byteBuffer.put(command);
            file.append(byteBuffer, outLength);
        } finally {
            byteBufferPool.recycle(byteBuffer);
        }
        return offset;
    }

    public long[] append(List<Log> logs) {
        long[] offsets = new long[logs.size()];
        long base = file.size();
        long offset = base;
        for (int i = 0; i < logs.size(); i++) {
            offsets[i] = offset;
            offset += (LOG_ENTRY_HEADER_SIZE + logs.get(i).getCommand().length);
        }
        int contentLength = (int) (offset - base);
        PooledByteBuffer byteBuffer = byteBufferPool.allocate(contentLength);
        try {
            for (Log log : logs) {
                byteBuffer.putInt(log.getIndex());
                byteBuffer.putInt(log.getTerm());
                byteBuffer.putInt(log.getType());
                byte[] command = log.getCommand();
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
    public Log getEntry(long start, long end) {
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
        PooledByteBuffer byteBuffer = byteBufferPool.allocate(readLength);
        try {
            file.readByteBufferAt(start, byteBuffer, readLength);
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
            byte[] cmd = new byte[contentLength];
            byteBuffer.get(cmd, 0, contentLength);
            return LogFactory.createEntry(type, term, index, cmd, contentLength);
        } finally {
            byteBufferPool.recycle(byteBuffer);
        }
    }

    public void loadEntriesIntoList(long start, long end, List<Log> res) {
        int size = (int) (end - start);
        PooledByteBuffer byteBuffer = byteBufferPool.allocate(size);
        try {
            file.readByteBufferAt(start, byteBuffer, size);
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
                res.add(LogFactory.createEntry(type, term, index, cmd, cmdLength));
            }
        } finally {
            byteBufferPool.recycle(byteBuffer);
        }
    }

    public PooledByteBuffer loadAll() {
        int contentLength = (int) file.size();
        PooledByteBuffer byteBuffer = byteBufferPool.allocate(contentLength);
        file.readByteBufferAt(0, byteBuffer, contentLength);
        return byteBuffer;
    }

    public void transferTo(long offset, Path dst) throws IOException {
        Files.deleteIfExists(dst);
        FileChannel dstChannel = FileChannel.open(dst, READ, WRITE, DSYNC, CREATE);
        file.transferTo(offset, file.size() - offset, dstChannel);
    }

    public void removeAfter(long offset) {
        file.truncate(offset < 0 ? 0L : offset);
    }

    public void delete() {
        file.delete();
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
