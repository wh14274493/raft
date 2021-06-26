package cn.ttplatform.wh.data.log;

import cn.ttplatform.wh.data.tool.Bits;
import cn.ttplatform.wh.data.tool.ByteBufferWriter;
import cn.ttplatform.wh.data.tool.ReadableAndWriteableFile;
import cn.ttplatform.wh.exception.IncorrectLogIndexNumberException;
import cn.ttplatform.wh.support.LRU;
import cn.ttplatform.wh.support.Pool;
import java.io.File;
import java.nio.ByteBuffer;
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
public final class LogIndexFile implements LogIndexOperation{


    private final LogIndexCache logIndexCache;
    private final Pool<ByteBuffer> byteBufferPool;
    private final ReadableAndWriteableFile file;
    private int minIndex;
    private int maxIndex;

    public LogIndexFile(File file, Pool<ByteBuffer> byteBufferPool, int lastIncludeIndex) {
        this.file = new ByteBufferWriter(file, byteBufferPool);
        this.logIndexCache = new LogIndexCache(100);
        this.byteBufferPool = byteBufferPool;
        this.minIndex = lastIncludeIndex;
        this.maxIndex = lastIncludeIndex;
        initialize();
    }

    @Override
    public void initialize() {
        if (!isEmpty()) {
            ByteBuffer byteBuffer = byteBufferPool.allocate(LogIndex.BYTES);
            try {
                this.file.readBytes(0L, byteBuffer, LogIndex.BYTES);
                this.minIndex = Bits.getInt(byteBuffer);
                logIndexCache.put(minIndex, LogIndex.builder().index(minIndex).term(Bits.getInt(byteBuffer))
                    .type(Bits.getInt(byteBuffer)).offset(Bits.getLong(byteBuffer)).build());
                byteBuffer.clear();
                this.file.readBytes(this.file.size() - LogIndex.BYTES, byteBuffer, LogIndex.BYTES);
                this.maxIndex = Bits.getInt(byteBuffer);
                logIndexCache.put(maxIndex, LogIndex.builder().index(maxIndex).term(Bits.getInt(byteBuffer))
                    .type(Bits.getInt(byteBuffer)).offset(Bits.getLong(byteBuffer)).build());
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
        ByteBuffer byteBuffer = byteBufferPool.allocate(LogIndex.BYTES);
        try {
            long position = (long) (index - minIndex) * LogIndex.BYTES;
            this.file.readBytes(position, byteBuffer, LogIndex.BYTES);
            return LogIndex.builder().index(Bits.getInt(byteBuffer)).term(Bits.getInt(byteBuffer)).type(Bits.getInt(byteBuffer))
                .offset(Bits.getLong(byteBuffer)).build();
        } finally {
            byteBufferPool.recycle(byteBuffer);
        }
    }

    @Override
    public int getMaxIndex() {
        return maxIndex;
    }

    @Override
    public int getMinIndex() {
        return minIndex;
    }

    @Override
    public LogIndex getLastLogMetaData() {
        return getLogMetaData(maxIndex);
    }

    @Override
    public LogIndex getLogMetaData(int index) {
        return Optional.ofNullable(logIndexCache.get(index)).orElse(loadLogIndex(index));
    }

    @Override
    public long getEntryOffset(int index) {
        LogIndex logIndex = getLogMetaData(index);
        return logIndex != null ? logIndex.getOffset() : -1L;
    }

    @Override
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
        ByteBuffer byteBuffer = byteBufferPool.allocate(LogIndex.BYTES);
        try {
            Bits.putInt(log.getIndex(), byteBuffer);
            Bits.putInt(log.getTerm(), byteBuffer);
            Bits.putInt(log.getType(), byteBuffer);
            Bits.putLong(logIndex.getOffset(), byteBuffer);
            file.append(byteBuffer, LogIndex.BYTES);
            logIndexCache.put(logIndex.getIndex(), logIndex);
        } finally {
            byteBufferPool.recycle(byteBuffer);
        }
    }

    @Override
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
        int contentLength = logs.size() * LogIndex.BYTES;
        ByteBuffer byteBuffer = byteBufferPool.allocate(contentLength);
        try {
            Log log;
            for (int i = 0; i < logs.size(); i++) {
                log = logs.get(i);
                Bits.putInt(log.getIndex(), byteBuffer);
                Bits.putInt(log.getTerm(), byteBuffer);
                Bits.putInt(log.getType(), byteBuffer);
                Bits.putLong(offsets[i], byteBuffer);
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

    @Override
    public void append(ByteBuffer byteBuffer) {
        file.append(byteBuffer, byteBuffer.position());
    }

    @Override
    public void removeAfter(int index) {
        if (index < minIndex) {
            file.clear();
            logIndexCache.clear();
            minIndex = 0;
            maxIndex = 0;
        } else if (index < maxIndex) {
            long position = (long) (index - minIndex + 1) * LogIndex.BYTES;
            file.truncate(position);
            maxIndex = index;
            logIndexCache.removeAfter(index);
        }
    }

    @Override
    public boolean isEmpty() {
        return file.isEmpty();
    }

    @Override
    public long size() {
        return file.size();
    }

    @Override
    public void close() {
        file.close();
    }

    static class LogIndexCache extends LRU<Integer, LogIndex> {

        public LogIndexCache(int capacity) {
            super(capacity);
        }

        public void removeAfter(int index) {
            Iterator<Map.Entry<Integer, KVEntry<Integer, LogIndex>>> iterator = cache.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<Integer, KVEntry<Integer, LogIndex>> next = iterator.next();
                if (next.getKey() > index) {
                    KVEntry<Integer, LogIndex> kvEntry = next.getValue();
                    remove(kvEntry);
                    iterator.remove();
                    used--;
                }
            }
        }
    }
}
