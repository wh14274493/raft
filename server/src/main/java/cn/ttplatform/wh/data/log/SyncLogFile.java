package cn.ttplatform.wh.data.log;

import cn.ttplatform.wh.data.support.Bits;
import cn.ttplatform.wh.data.support.LogFileMetadataRegion;
import cn.ttplatform.wh.data.support.SyncFileOperator;
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
public class SyncLogFile implements LogOperation {

    private final SyncFileOperator fileOperator;
    private final Pool<ByteBuffer> byteBufferPool;
    private LogFileMetadataRegion logFileMetadataRegion;

    public SyncLogFile(File file, Pool<ByteBuffer> byteBufferPool, LogFileMetadataRegion logFileMetadataRegion) {
        this.logFileMetadataRegion = logFileMetadataRegion;
        this.fileOperator = new SyncFileOperator(file, byteBufferPool, logFileMetadataRegion);
        this.byteBufferPool = byteBufferPool;
    }

    @Override
    public long append(Log log) {
        long offset = fileOperator.size();
        byte[] command = log.getCommand();
        int length = LOG_HEADER_SIZE + command.length;
        ByteBuffer byteBuffer = byteBufferPool.allocate(length);
        try {
            Bits.putInt(log.getIndex(), byteBuffer);
            Bits.putInt(log.getTerm(), byteBuffer);
            Bits.putInt(log.getType(), byteBuffer);
            Bits.putInt(command.length, byteBuffer);
            byteBuffer.put(command);
            fileOperator.append(byteBuffer, length);
        } finally {
            byteBufferPool.recycle(byteBuffer);
        }
        return offset;
    }

    @Override
    public long[] append(List<Log> logs) {
        long[] offsets = new long[logs.size()];
        long base = fileOperator.size();
        long offset = base;
        for (int i = 0; i < logs.size(); i++) {
            offsets[i] = offset;
            offset += (LOG_HEADER_SIZE + logs.get(i).getCommand().length);
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
            fileOperator.append(byteBuffer, contentLength);
        } finally {
            byteBufferPool.recycle(byteBuffer);
        }
        return offsets;
    }

    @Override
    public void append(ByteBuffer byteBuffer, int length) {
        fileOperator.append(byteBuffer, length);
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
        long size = fileOperator.size();
        if (start >= size || start == end) {
            log.debug("can not load a log from offset[{}] to offset[{}].", start, end);
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
            fileOperator.readBytes(start, byteBuffer, readLength);
            // Convert a byte array to {@link Log}
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
            return LogFactory.createEntry(type, term, index, cmd);
        } finally {
            byteBufferPool.recycle(byteBuffer);
        }
    }

    @Override
    public void loadLogsIntoList(long start, long end, List<Log> res) {
        int size = (int) (end - start);
        ByteBuffer byteBuffer = byteBufferPool.allocate(size);
        try {
            fileOperator.readBytes(start, byteBuffer, size);
            int offset = 0;
            while (offset < size) {
                int index = Bits.getInt(byteBuffer);
                int term = Bits.getInt(byteBuffer);
                int type = Bits.getInt(byteBuffer);
                int cmdLength = Bits.getInt(byteBuffer);
                byte[] cmd = null;
                if (cmdLength > 0) {
                    cmd = new byte[cmdLength];
                    byteBuffer.get(cmd, 0, cmdLength);
                }
                offset += cmdLength + LOG_HEADER_SIZE;
                res.add(LogFactory.createEntry(type, term, index, cmd));
            }
        } finally {
            byteBufferPool.recycle(byteBuffer);
        }
    }

    @Override
    public ByteBuffer[] read() {
        int position = 0;
        int fileSize = (int) fileOperator.size();
        int size = fileSize % MAX_CHUNK_SIZE == 0 ? fileSize / MAX_CHUNK_SIZE : fileSize / MAX_CHUNK_SIZE + 1;
        ByteBuffer[] buffers = new ByteBuffer[size];
        int index = 0;
        while (position < fileSize) {
            int readLength = Math.min(fileSize - position, MAX_CHUNK_SIZE);
            ByteBuffer byteBuffer = byteBufferPool.allocate(MAX_CHUNK_SIZE);
            fileOperator.readBytes(position, byteBuffer, readLength);
            position += readLength;
            buffers[index++] = byteBuffer;
        }
        return buffers;
    }

    @Override
    public void transferTo(long offset, LogOperation dst) {
        long fileSize = fileOperator.size();
        int contentLength = (int) (fileSize - offset);
        ByteBuffer byteBuffer = byteBufferPool.allocate(contentLength);
        try {
            fileOperator.readBytes(offset, byteBuffer, contentLength);
            dst.append(byteBuffer, contentLength);
        } finally {
            byteBufferPool.recycle(byteBuffer);
        }
    }

    @Override
    public void exchangeLogFileMetadataRegion(LogFileMetadataRegion logFileMetadataRegion) {
        logFileMetadataRegion.write(this.logFileMetadataRegion.read());
        this.logFileMetadataRegion.clear();
        this.logFileMetadataRegion = logFileMetadataRegion;
        fileOperator.changeFileHeaderOperator(logFileMetadataRegion);
    }

    @Override
    public void removeAfter(long offset) {
        if (offset <= 0) {
            fileOperator.truncate(0);
        }
        fileOperator.truncate(offset);
    }

    @Override
    public void close() {
        fileOperator.close();
    }

    @Override
    public long size() {
        return fileOperator.size();
    }

    @Override
    public boolean isEmpty() {
        return fileOperator.isEmpty();
    }
}
