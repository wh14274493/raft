package cn.ttplatform.wh.core.log.entry;

import static cn.ttplatform.wh.core.support.ByteConvertor.fillIntBytes;
import static cn.ttplatform.wh.core.support.ByteConvertor.fillLongBytes;

import cn.ttplatform.wh.constant.FileName;
import cn.ttplatform.wh.core.log.LogFactory;
import cn.ttplatform.wh.core.support.BufferPool;
import cn.ttplatform.wh.core.support.ByteBufferWriter;
import cn.ttplatform.wh.core.support.ReadableAndWriteableFile;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Wang Hao
 * @date 2020/7/1 下午10:12
 */
@Slf4j
public class FileLogEntry {

    public static final int LOG_ENTRY_HEADER_SIZE = 16;
    private final LogFactory logFactory = LogFactory.getInstance();
    private final ReadableAndWriteableFile file;

    public FileLogEntry(File parent, BufferPool<ByteBuffer> pool) {
        this.file = new ByteBufferWriter(new File(parent, FileName.LOG_ENTRY_FILE_NAME), pool);
    }

    public long append(LogEntry logEntry) {
        long offset = file.size();
        file.writeBytes(logFactory.transferLogEntryToBytes(logEntry));
        return offset;
    }

    public List<Long> append(List<LogEntry> logEntries) {
        List<Long> offsetList = new ArrayList<>(logEntries.size());
        long base = file.size();
        long offset = base;
        for (LogEntry logEntry : logEntries) {
            offsetList.add(offset);
            offset += (LOG_ENTRY_HEADER_SIZE + logEntry.getCommand().length);
        }
        byte[] content = new byte[(int) (offset - base)];
        fillContentWithLogEntries(content, logEntries);
        file.append(content);
        return offsetList;
    }

    public void fillContentWithLogEntries(byte[] content, List<LogEntry> logEntries) {
        int index = 0;
        for (LogEntry logEntry : logEntries) {
            index += 3;
            fillIntBytes(logEntry.getIndex(), content, index);
            index += 4;
            fillIntBytes(logEntry.getTerm(), content, index);
            index += 4;
            fillIntBytes(logEntry.getType(), content, index);
            index += 4;
            byte[] command = logEntry.getCommand();
            fillLongBytes(command.length, content, index);
            index++;
            for (byte b : command) {
                content[index++] = b;
            }
        }
    }

    public LogEntry getEntry(long start, long end) {
        long fileSize = file.size();
        if (start < 0 || start > fileSize) {
            return null;
        }
        int readLength;
        if (end < start) {
            readLength = (int) (fileSize - start);
        } else {
            readLength = (int) (end - start);
        }
        byte[] content = file.readBytesAt(start, readLength);
        return logFactory.transferBytesToLogEntry(content, 0);
    }

    public byte[] loadEntriesFromFile(long start, long end) {
        if (end == -1L) {
            end = file.size();
        }
        return file.readBytesAt(start, (int) (end - start));
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
