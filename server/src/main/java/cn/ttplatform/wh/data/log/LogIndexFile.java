package cn.ttplatform.wh.data.log;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.DSYNC;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

import cn.ttplatform.wh.data.tool.ByteBufferWriter;
import cn.ttplatform.wh.data.tool.PooledByteBuffer;
import cn.ttplatform.wh.data.tool.ReadableAndWriteableFile;
import cn.ttplatform.wh.exception.IncorrectLogIndexNumberException;
import cn.ttplatform.wh.support.LRUCache;
import cn.ttplatform.wh.support.Pool;
import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Wang Hao
 * @date 2020/7/1 下午10:13
 */
@Slf4j
public final class LogIndexFile {

    /**
     * index(4 bytes) + term(4 bytes) + type(4 bytes) + offset(8 bytes) = 20
     */
    private static final int ITEM_LENGTH = Integer.BYTES * 3 + Long.BYTES;
    private final LogIndexCache logIndexCache;
    private final Pool<PooledByteBuffer> byteBufferPool;
    private final ReadableAndWriteableFile file;
    private int minIndex;
    private int maxIndex;

    public LogIndexFile(File file, Pool<PooledByteBuffer> byteBufferPool, int lastIncludeIndex) {
        this.file = new ByteBufferWriter(file, byteBufferPool);
        this.logIndexCache = new LogIndexCache(100);
        this.byteBufferPool = byteBufferPool;
        this.minIndex = lastIncludeIndex;
        this.maxIndex = lastIncludeIndex;
        initialize();
    }

    public void initialize() {
        if (!isEmpty()) {
            PooledByteBuffer byteBuffer = byteBufferPool.allocate(ITEM_LENGTH);
            try {
                this.file.readByteBufferAt(0L, byteBuffer, ITEM_LENGTH);
                this.minIndex = byteBuffer.getInt();
                logIndexCache.put(minIndex, LogIndex.builder().index(minIndex).term(byteBuffer.getInt()).type(byteBuffer.getInt())
                    .offset(byteBuffer.getLong()).build());
                byteBuffer.clear();
                this.file.readByteBufferAt(this.file.size() - ITEM_LENGTH, byteBuffer, ITEM_LENGTH);
                this.maxIndex = byteBuffer.getInt();
                logIndexCache.put(maxIndex, LogIndex.builder().index(maxIndex).term(byteBuffer.getInt()).type(byteBuffer.getInt())
                    .offset(byteBuffer.getLong()).build());
            } finally {
                byteBufferPool.recycle(byteBuffer);
            }
        }
        log.info("initialize minIndex = {}, maxIndex = {}.", minIndex, maxIndex);
    }

    private LogIndex loadLogIndex(int index) {
        if (index < minIndex || index > maxIndex) {
            return null;
        }
        PooledByteBuffer byteBuffer = byteBufferPool.allocate(ITEM_LENGTH);
        LogIndex logIndex;
        try {
            long position = (long) (index - minIndex) * ITEM_LENGTH;
            this.file.readByteBufferAt(position, byteBuffer, ITEM_LENGTH);
            logIndex = LogIndex.builder().index(byteBuffer.getInt()).term(byteBuffer.getInt()).type(byteBuffer.getInt())
                .offset(byteBuffer.getLong()).build();
        } finally {
            byteBufferPool.recycle(byteBuffer);
        }
        return logIndex;
    }

    public int getMaxIndex() {
        return maxIndex;
    }

    public int getMinIndex() {
        return minIndex;
    }

    public LogIndex getLastLogMetaData() {
        return getLogMetaData(maxIndex);
    }

    public LogIndex getLogMetaData(int index) {
        return Optional.ofNullable(logIndexCache.get(index)).orElse(loadLogIndex(index));
    }

    public long getEntryOffset(int index) {
        LogIndex logIndex = getLogMetaData(index);
        return logIndex != null ? logIndex.getOffset() : -1L;
    }

    public void append(Log log, long offset) {
        int index = log.getIndex();
        if (isEmpty()) {
            minIndex = index;
        } else {
            if (index != maxIndex + 1) {
                throw new IncorrectLogIndexNumberException("index[" + index + "] is not correct, maxEntryIndex is " + maxIndex);
            }
        }
        maxIndex = index;
        LogIndexFile.log.debug("update maxLogIndex = {}.", maxIndex);
        LogIndex logIndex = LogIndex.builder().index(index).term(log.getTerm()).offset(offset).type(log.getType()).build();
        PooledByteBuffer byteBuffer = byteBufferPool.allocate(ITEM_LENGTH);
        try {
            byteBuffer.putInt(logIndex.getIndex());
            byteBuffer.putInt(logIndex.getTerm());
            byteBuffer.putInt(logIndex.getType());
            byteBuffer.putLong(logIndex.getOffset());
            file.append(byteBuffer, ITEM_LENGTH);
            logIndexCache.put(logIndex.getIndex(), logIndex);
        } finally {
            byteBufferPool.recycle(byteBuffer);
        }
    }

    public void append(List<Log> logs, long[] offsets) {
        if (isEmpty()) {
            minIndex = logs.get(0).getIndex();
        } else {
            if (logs.get(0).getIndex() != maxIndex + 1) {
                throw new IllegalArgumentException(
                    "index[" + logs.get(0).getIndex() + "] is not correct, maxEntryIndex is " + maxIndex);
            }
        }
        maxIndex = logs.get(logs.size() - 1).getIndex();
        log.debug("update maxLogIndex = {}.", maxIndex);
        int contentLength = logs.size() * ITEM_LENGTH;
        PooledByteBuffer byteBuffer = byteBufferPool.allocate(contentLength);
        try {
            Log log;
            for (int i = 0; i < logs.size(); i++) {
                log = logs.get(i);
                byteBuffer.putInt(log.getIndex());
                byteBuffer.putInt(log.getTerm());
                byteBuffer.putInt(log.getType());
                byteBuffer.putLong(offsets[i]);
                LogIndex logIndex = LogIndex.builder()
                    .index(log.getIndex())
                    .term(log.getTerm())
                    .type(log.getType())
                    .offset(offsets[i])
                    .build();
                logIndexCache.put(logIndex.getIndex(), logIndex);
            }
            file.append(byteBuffer, contentLength);
        } finally {
            byteBufferPool.recycle(byteBuffer);
        }
    }

    public void append(PooledByteBuffer byteBuffer) {
        file.append(byteBuffer,byteBuffer.position());
    }

    public void removeAfter(int index) {
        if (index < minIndex) {
            file.clear();
            logIndexCache.clear();
            minIndex = 0;
            maxIndex = 0;
        } else if (index < maxIndex) {
            long position = (long) (index - minIndex + 1) * ITEM_LENGTH;
            file.truncate(position);
            maxIndex = index;
            logIndexCache.removeAfter(index);
        }
    }

    public void transferTo(int index, Path dst) throws IOException {
        Files.deleteIfExists(dst);
        FileChannel dstChannel = FileChannel.open(dst, READ, WRITE, DSYNC, CREATE);
        long offset = (long) ITEM_LENGTH * (index - 1);
        file.transferTo(offset, file.size() - offset, dstChannel);
    }

    public void delete() {
        file.delete();
    }

    public boolean isEmpty() {
        return file.isEmpty();
    }

    public long size() {
        return file.size();
    }

    public void close() {
        file.close();
    }

    static class LogIndexCache extends LRUCache<Integer, LogIndex> {

        public LogIndexCache(int capacity) {
            super(capacity);
        }

        public void removeAfter(int index) {
            Iterator<Map.Entry<Integer, Entry<Integer, LogIndex>>> iterator = cache.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<Integer, Entry<Integer, LogIndex>> next = iterator.next();
                if (next.getKey() > index) {
                    Entry<Integer, LogIndex> entry = next.getValue();
                    remove(entry);
                    iterator.remove();
                    size--;
                }
            }
        }
    }
}
