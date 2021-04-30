package cn.ttplatform.wh.core.log.entry;

import static cn.ttplatform.wh.core.log.tool.ByteConvertor.fillIntBytes;

import cn.ttplatform.wh.core.log.generation.FileName;
import cn.ttplatform.wh.core.log.tool.ByteBufferWriter;
import cn.ttplatform.wh.core.log.tool.ReadableAndWriteableFile;
import cn.ttplatform.wh.support.BufferPool;
import java.io.File;
import java.nio.ByteBuffer;
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
    private final LogEntryFactory logEntryFactory = LogEntryFactory.getInstance();
    private final ReadableAndWriteableFile file;

    public FileLogEntry(File parent, BufferPool<ByteBuffer> pool) {
        this.file = new ByteBufferWriter(new File(parent, FileName.LOG_ENTRY_FILE_NAME), pool);
    }

    public long append(LogEntry logEntry) {
        long offset = file.size();
        file.append(logEntryFactory.transferLogEntryToBytes(logEntry));
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
        byte[] content = new byte[(int) (offset - base)];
        fillContentWithLogEntries(content, logEntries);
        file.append(content);
        return offsets;
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
            fillIntBytes(command.length, content, index);
            index++;
            for (byte b : command) {
                content[index++] = b;
            }
        }
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
        if (start < 0 || start > size) {
            return null;
        }
        int readLength;
        if (end < start) {
            readLength = (int) (size - start);
        } else {
            readLength = (int) (end - start);
        }
        byte[] content = file.readBytesAt(start, readLength);
        return logEntryFactory.transferBytesToLogEntry(content);
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
