package cn.ttplatform.wh.core.log.entry;

import static cn.ttplatform.wh.core.log.tool.ByteConvertor.fillIntBytes;
import static cn.ttplatform.wh.core.log.tool.ByteConvertor.fillLongBytes;

import cn.ttplatform.wh.core.log.generation.FileName;
import cn.ttplatform.wh.core.log.tool.ByteBufferWriter;
import cn.ttplatform.wh.core.log.tool.ReadableAndWriteableFile;
import cn.ttplatform.wh.support.Pool;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Wang Hao
 * @date 2020/7/1 下午10:13
 */
@Slf4j
@Getter
public final class FileLogEntryIndex {

    public static final int ITEM_LENGTH = Integer.BYTES * 3 + Long.BYTES;
    private static final long INDEX_LENGTH = Integer.BYTES;
    private final ReadableAndWriteableFile file;
    private int minLogIndex;
    private int maxLogIndex;
    private final List<LogEntryIndex> logEntryIndices = new ArrayList<>();
    private final Pool<byte[]> byteArrayPool;
    private final LogEntryFactory logEntryFactory = LogEntryFactory.getInstance();

    public FileLogEntryIndex(File parent, Pool<ByteBuffer> byteBufferPool, Pool<byte[]> byteArrayPool, int lastIncludeIndex) {
        file = new ByteBufferWriter(new File(parent, FileName.INDEX_FILE_NAME), byteBufferPool, byteArrayPool);
        this.byteArrayPool = byteArrayPool;
        minLogIndex = lastIncludeIndex;
        maxLogIndex = lastIncludeIndex;
        initialize();
    }

    private void initialize() {
        if (!isEmpty()) {
            byte[] content = file.readBytesAt(0L, (int) file.size());
            LogEntryIndex logEntryIndex = null;
            for (int index = 0; index < content.length; index += ITEM_LENGTH) {
                logEntryIndex = logEntryFactory.transferBytesToLogEntryIndex(content, index);
                logEntryIndices.add(logEntryIndex);
                if (index == 0) {
                    minLogIndex = logEntryIndex.getIndex();
                }
            }
            if (logEntryIndex != null) {
                maxLogIndex = logEntryIndex.getIndex();
            }
        }
    }

    public LogEntryIndex getLastEntryIndex() {
        return logEntryIndices.get(maxLogIndex - minLogIndex);
    }

    public long getEntryOffset(int index) {
        LogEntryIndex logEntryIndex = getEntryMetaData(index);
        return logEntryIndex != null ? logEntryIndex.getOffset() : -1L;
    }

    public LogEntryIndex getEntryMetaData(int index) {
        if (index < minLogIndex || index > maxLogIndex) {
            return null;
        }
        return logEntryIndices.get(index - minLogIndex);
    }

    public void append(LogEntry logEntry, long offset) {
        int index = logEntry.getIndex();
        if (isEmpty()) {
            minLogIndex = index;
        } else {
            if (index != maxLogIndex + 1) {
                throw new IllegalArgumentException(
                    "index[" + index + "] is not correct, maxEntryIndex is " + maxLogIndex);
            }
        }
        maxLogIndex = index;
        log.debug("update maxLogIndex = {}.", maxLogIndex);
        LogEntryIndex logEntryIndex = LogEntryIndex.builder().index(index).term(logEntry.getTerm()).offset(offset)
            .type(logEntry.getType()).build();
        byte[] res = byteArrayPool.allocate(FileLogEntryIndex.ITEM_LENGTH);
        try {
            transferLogEntryIndexToBytes(logEntryIndex, res);
            file.append(res, FileLogEntryIndex.ITEM_LENGTH);
            logEntryIndices.add(logEntryIndex);
        } finally {
            byteArrayPool.recycle(res);
        }
    }

    /**
     * Convert a {@link LogEntryIndex} object to byte array
     *
     * @param logEntryIndex source object
     */
    private void transferLogEntryIndexToBytes(LogEntryIndex logEntryIndex, byte[] res) {
        // index[0-3]
        fillIntBytes(logEntryIndex.getIndex(), res, 3);
        // term[4-7]
        fillIntBytes(logEntryIndex.getTerm(), res, 7);
        // term[8-11]
        fillIntBytes(logEntryIndex.getType(), res, 11);
        // term[12-19]
        fillLongBytes(logEntryIndex.getOffset(), res, 19);
    }

    public void append(List<LogEntry> logEntries, long[] offsets) {
        if (isEmpty()) {
            minLogIndex = logEntries.get(0).getIndex();
        } else {
            if (logEntries.get(0).getIndex() != maxLogIndex + 1) {
                throw new IllegalArgumentException(
                    "index[" + logEntries.get(0).getIndex() + "] is not correct, maxEntryIndex is " + maxLogIndex);
            }
        }
        maxLogIndex = logEntries.get(logEntries.size() - 1).getIndex();
        log.debug("update maxLogIndex = {}.", maxLogIndex);
        int contentLength = logEntries.size() * ITEM_LENGTH;
        byte[] content = byteArrayPool.allocate(contentLength);
        try {
            fillContentWithLogEntryIndex(content, logEntries, offsets);
            file.append(content, contentLength);
        } finally {
            byteArrayPool.recycle(content);
        }
    }

    private void fillContentWithLogEntryIndex(byte[] content, List<LogEntry> logEntries, long[] offsets) {
        int index = 0;
        LogEntry logEntry;
        for (int i = 0; i < logEntries.size(); i++) {
            logEntry = logEntries.get(i);
            index += 3;
            fillIntBytes(logEntry.getIndex(), content, index);
            index += 4;
            fillIntBytes(logEntry.getTerm(), content, index);
            index += 4;
            fillIntBytes(logEntry.getType(), content, index);
            index += 8;
            fillLongBytes(offsets[i], content, index);
            index++;
            logEntryIndices.add(LogEntryIndex.builder()
                .index(logEntry.getIndex())
                .term(logEntry.getTerm())
                .offset(offsets[i])
                .type(logEntry.getType())
                .build());
        }
    }

    public void removeAfter(int index) {
        if (index < minLogIndex) {
            file.clear();
            logEntryIndices.clear();
            minLogIndex = 0;
            maxLogIndex = 0;
        } else if (index < maxLogIndex) {
            long position = (long) (index - minLogIndex + 1) * ITEM_LENGTH;
            file.truncate(position);
            maxLogIndex = index;
            int first = index - minLogIndex;
            if (maxLogIndex - minLogIndex >= first + 1) {
                logEntryIndices.subList(first + 1, maxLogIndex - minLogIndex + 1).clear();
            }
        }
    }

    public boolean isEmpty() {
        return file.isEmpty();
    }

    public void close() {
        file.close();
    }
}
