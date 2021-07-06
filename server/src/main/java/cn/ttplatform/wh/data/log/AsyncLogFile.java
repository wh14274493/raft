package cn.ttplatform.wh.data.log;

import cn.ttplatform.wh.config.ServerProperties;
import cn.ttplatform.wh.data.support.AsyncFileOperator;
import cn.ttplatform.wh.support.Pool;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

/**
 * @author Wang Hao
 * @date 2021/6/8 23:50
 */
@Slf4j
public class AsyncLogFile implements LogOperation {

    private final AsyncFileOperator fileOperator;
    private LogFileMetadataRegion logFileMetadataRegion;

    public AsyncLogFile(File file, ServerProperties properties, Pool<ByteBuffer> byteBufferPool, LogFileMetadataRegion logFileMetadataRegion) {
        this.logFileMetadataRegion = logFileMetadataRegion;
        this.fileOperator = new AsyncFileOperator(properties, byteBufferPool, file);
    }

    @Override
    public long append(Log log) {
        long offset = logFileMetadataRegion.getFileSize();
        long fileSize = append0(offset, log);
        logFileMetadataRegion.recordFileSize(fileSize);
        return offset;
    }

    private long append0(long offset, Log log) {
        fileOperator.appendInt(offset, log.getIndex());
        offset += 4;
        fileOperator.appendInt(offset, log.getTerm());
        offset += 4;
        fileOperator.appendInt(offset, log.getType());
        offset += 4;
        byte[] command = log.getCommand();
        fileOperator.appendInt(offset, command.length);
        offset += 4;
        fileOperator.appendBytes(offset, command);
        offset += command.length;
        return offset;
    }

    @Override
    public long[] append(List<Log> logs) {
        long[] offsets = new long[logs.size()];
        long fileSize = logFileMetadataRegion.getFileSize();
        for (int i = 0; i < logs.size(); i++) {
            offsets[i] = fileSize;
            fileSize = append0(fileSize, logs.get(i));
        }
        logFileMetadataRegion.recordFileSize(fileSize);
        return offsets;
    }

    @Override
    public void append(ByteBuffer byteBuffer, int length) {
        long fileSize = logFileMetadataRegion.getFileSize();
        fileOperator.appendBlock(fileSize, byteBuffer);
        logFileMetadataRegion.recordFileSize(fileSize + length);
    }

    @Override
    public Log getLog(long start, long end) {
        int index = fileOperator.getInt(start);
        start += 4;
        int term = fileOperator.getInt(start);
        start += 4;
        int type = fileOperator.getInt(start);
        start += 4;
        int cmdLength = fileOperator.getInt(start);
        start += 4;
        byte[] cmd = null;
        if (cmdLength > 0) {
            cmd = new byte[cmdLength];
            fileOperator.get(start, cmd);
        }
        return LogFactory.createEntry(type, term, index, cmd);
    }

    @Override
    public void loadLogsIntoList(long start, long end, List<Log> res) {
        int index;
        int term;
        int type;
        int cmdLength;
        byte[] cmd;
        while (start < end) {
            index = fileOperator.getInt(start);
            start += 4;
            term = fileOperator.getInt(start);
            start += 4;
            type = fileOperator.getInt(start);
            start += 4;
            cmdLength = fileOperator.getInt(start);
            start += 4;
            if (cmdLength == 0) {
                cmd = null;
            } else {
                cmd = new byte[cmdLength];
                fileOperator.get(start, cmd);
            }
            start += cmdLength;
            res.add(LogFactory.createEntry(type, term, index, cmd));
        }
    }

    @Override
    public ByteBuffer[] read() {
        return fileOperator.readBytes(0L, logFileMetadataRegion.getFileSize());
    }

    @Override
    public void transferTo(long offset, LogOperation logOperation) {
        ByteBuffer[] buffers = fileOperator.readBytes(offset, logFileMetadataRegion.getFileSize());
        Arrays.stream(buffers).forEach(buffer -> logOperation.append(buffer, buffer.limit()));
    }

    @Override
    public void exchangeLogFileMetadataRegion(LogFileMetadataRegion logFileMetadataRegion) {
        logFileMetadataRegion.recordFileSize(this.logFileMetadataRegion.getFileSize());
        this.logFileMetadataRegion.clear();
        this.logFileMetadataRegion = logFileMetadataRegion;
    }

    @Override
    public void removeAfter(long offset) {
        fileOperator.truncate(offset, logFileMetadataRegion.getFileSize());
        logFileMetadataRegion.recordFileSize(offset);
    }

    @Override
    public void close() {
        fileOperator.close();
    }

    @Override
    public long size() {
        return logFileMetadataRegion.getFileSize();
    }

    @Override
    public boolean isEmpty() {
        return logFileMetadataRegion.getFileSize() <= 0;
    }


}
