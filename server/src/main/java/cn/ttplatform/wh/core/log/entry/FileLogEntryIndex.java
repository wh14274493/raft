package cn.ttplatform.wh.core.log.entry;

import cn.ttplatform.wh.core.log.generation.FileName;
import cn.ttplatform.wh.core.log.tool.ByteBufferWriter;
import cn.ttplatform.wh.support.PooledByteBuffer;
import cn.ttplatform.wh.core.log.tool.ReadableAndWriteableFile;
import cn.ttplatform.wh.support.Pool;
import java.io.File;
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
    private final Pool<PooledByteBuffer> byteBufferPool;
    private final Pool<byte[]> byteArrayPool;
    private final LogEntryFactory logEntryFactory = LogEntryFactory.getInstance();

    public FileLogEntryIndex(File parent, Pool<PooledByteBuffer> byteBufferPool, Pool<byte[]> byteArrayPool,
        int lastIncludeIndex) {
        this.file = new ByteBufferWriter(new File(parent, FileName.INDEX_FILE_NAME), byteBufferPool, byteArrayPool);
        this.byteBufferPool = byteBufferPool;
        this.byteArrayPool = byteArrayPool;
        this.minLogIndex = lastIncludeIndex;
        this.maxLogIndex = lastIncludeIndex;
        if (!isEmpty()) {
            PooledByteBuffer content = null;
            try {
                content = file.readByteBufferAt(0L, (int) file.size());
                content.flip();
                LogEntryIndex logEntryIndex = null;
                for (int index = 0; index < content.limit(); index += ITEM_LENGTH) {
                    logEntryIndex = LogEntryIndex.builder().index(content.getInt()).term(content.getInt()).type(content.getInt())
                        .offset(content.getLong()).build();
                    logEntryIndices.add(logEntryIndex);
                    if (index == 0) {
                        minLogIndex = logEntryIndex.getIndex();
                    }
                }
                if (logEntryIndex != null) {
                    maxLogIndex = logEntryIndex.getIndex();
                }
            } finally {
                byteBufferPool.recycle(content);
            }

        }
    }

    public LogEntryIndex getLastEntryIndex() {
        return logEntryIndices.get(logEntryIndices.size() - 1);
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
        PooledByteBuffer byteBuffer = byteBufferPool.allocate(FileLogEntryIndex.ITEM_LENGTH);
        try {
            byteBuffer.putInt(logEntryIndex.getIndex());
            byteBuffer.putInt(logEntryIndex.getTerm());
            byteBuffer.putInt(logEntryIndex.getType());
            byteBuffer.putLong(logEntryIndex.getOffset());
            file.append(byteBuffer, FileLogEntryIndex.ITEM_LENGTH);
            logEntryIndices.add(logEntryIndex);
        } finally {
            byteBufferPool.recycle(byteBuffer);
        }
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
        PooledByteBuffer byteBuffer = byteBufferPool.allocate(contentLength);
        try {
            LogEntry logEntry;
            for (int i = 0; i < logEntries.size(); i++) {
                logEntry = logEntries.get(i);
                byteBuffer.putInt(logEntry.getIndex());
                byteBuffer.putInt(logEntry.getTerm());
                byteBuffer.putInt(logEntry.getType());
                byteBuffer.putLong(offsets[i]);
                logEntryIndices.add(LogEntryIndex.builder()
                    .index(logEntry.getIndex())
                    .term(logEntry.getTerm())
                    .type(logEntry.getType())
                    .offset(offsets[i])
                    .build());
            }
            file.append(byteBuffer, contentLength);
        } finally {
            byteBufferPool.recycle(byteBuffer);
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
