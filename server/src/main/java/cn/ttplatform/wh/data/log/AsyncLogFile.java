package cn.ttplatform.wh.data.log;

import cn.ttplatform.wh.config.ServerProperties;
import cn.ttplatform.wh.data.support.AsyncFileOperator;
import cn.ttplatform.wh.data.support.LogFileMetadataRegion;
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
        this.fileOperator = new AsyncFileOperator(properties, byteBufferPool, file, logFileMetadataRegion);
    }

    @Override
    public long append(Log log) {
        long offset = fileOperator.getSize();
        fileOperator.appendInt(log.getIndex());
        fileOperator.appendInt(log.getTerm());
        fileOperator.appendInt(log.getType());
        byte[] command = log.getCommand();
        fileOperator.appendInt(command.length);
        fileOperator.appendBytes(command);
        return offset;
    }

    @Override
    public long[] append(List<Log> logs) {
        long[] offsets = new long[logs.size()];
        for (int i = 0; i < logs.size(); i++) {
            offsets[i] = fileOperator.getSize();
            append(logs.get(i));
        }
        return offsets;
    }

    @Override
    public void append(ByteBuffer byteBuffer, int length) {
        fileOperator.appendBlock(byteBuffer);
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
        return fileOperator.readBytes(0L);
    }

    @Override
    public void transferTo(long offset, LogOperation logOperation) {
        ByteBuffer[] buffers = fileOperator.readBytes(offset);
        Arrays.stream(buffers).forEach(buffer -> logOperation.append(buffer, buffer.limit()));
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
        fileOperator.truncate(offset);
    }

    @Override
    public void close() {
        fileOperator.close();
    }

    @Override
    public long size() {
        return fileOperator.getSize();
    }

    @Override
    public boolean isEmpty() {
        return fileOperator.isEmpty();
    }


}
