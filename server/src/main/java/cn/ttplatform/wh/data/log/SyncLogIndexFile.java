package cn.ttplatform.wh.data.log;

import cn.ttplatform.wh.data.support.Bits;
import cn.ttplatform.wh.data.support.LogIndexFileMetadataRegion;
import cn.ttplatform.wh.data.support.SyncFileOperator;
import cn.ttplatform.wh.exception.IncorrectLogIndexNumberException;
import cn.ttplatform.wh.support.LRU;
import cn.ttplatform.wh.support.Pool;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * @author Wang Hao
 * @date 2020/7/1 下午10:13
 */
@Slf4j
public final class SyncLogIndexFile implements LogIndexOperation {

    private final LogIndexCache logIndexCache;
    private final Pool<ByteBuffer> byteBufferPool;
    private final SyncFileOperator fileOperator;
    private LogIndexFileMetadataRegion logIndexFileMetadataRegion;
    private int minIndex;
    private int maxIndex;

    public SyncLogIndexFile(File file, LogIndexFileMetadataRegion logIndexFileMetadataRegion, Pool<ByteBuffer> byteBufferPool, int lastIncludeIndex) {
        this.logIndexFileMetadataRegion = logIndexFileMetadataRegion;
        this.fileOperator = new SyncFileOperator(file, byteBufferPool, logIndexFileMetadataRegion);
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
                this.fileOperator.readBytes(0, byteBuffer, LogIndex.BYTES);
                this.minIndex = Bits.getInt(byteBuffer);
                logIndexCache.put(minIndex, LogIndex.builder().index(minIndex).term(Bits.getInt(byteBuffer))
                        .type(Bits.getInt(byteBuffer)).offset(Bits.getLong(byteBuffer)).build());
                byteBuffer.clear();
                this.fileOperator.readBytes(fileOperator.size() - LogIndex.BYTES, byteBuffer, LogIndex.BYTES);
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
            fileOperator.readBytes(position, byteBuffer, LogIndex.BYTES);
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
    public long getLogOffset(int index) {
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
                throw new IncorrectLogIndexNumberException(String.format("index[%d] is not correct, maxEntryIndex is %d.", index, maxIndex));
            }
        }
        maxIndex = index;
        SyncLogIndexFile.log.debug("update maxLogIndex to {}.", maxIndex);
        LogIndex logIndex = log.getMetadata();
        logIndex.setOffset(offset);
        ByteBuffer byteBuffer = byteBufferPool.allocate(LogIndex.BYTES);
        try {
            Bits.putInt(log.getIndex(), byteBuffer);
            Bits.putInt(log.getTerm(), byteBuffer);
            Bits.putInt(log.getType(), byteBuffer);
            Bits.putLong(logIndex.getOffset(), byteBuffer);
            fileOperator.append(byteBuffer, LogIndex.BYTES);
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
                throw new IllegalArgumentException(String.format("index[%d] is not correct, maxEntryIndex is %d.", logs.get(0).getIndex(), maxIndex));
            }
        }
        maxIndex = logs.get(logs.size() - 1).getIndex();
        log.debug("update maxLogIndex to {}.", maxIndex);
        int contentLength = logs.size() * LogIndex.BYTES;
        ByteBuffer byteBuffer = byteBufferPool.allocate(contentLength);
        try {
            for (int i = 0; i < logs.size(); i++) {
                LogIndex logIndex = logs.get(i).getMetadata();
                logIndex.setOffset(offsets[i]);
                Bits.putInt(logIndex.getIndex(), byteBuffer);
                Bits.putInt(logIndex.getTerm(), byteBuffer);
                Bits.putInt(logIndex.getType(), byteBuffer);
                Bits.putLong(logIndex.getOffset(), byteBuffer);
                logIndexCache.put(logIndex.getIndex(), logIndex);
            }
            fileOperator.append(byteBuffer, contentLength);
        } finally {
            byteBufferPool.recycle(byteBuffer);
        }
    }

    @Override
    public void append(ByteBuffer byteBuffer) {
        fileOperator.append(byteBuffer, byteBuffer.limit());
    }

    @Override
    public void removeAfter(int index) {
        if (index < minIndex) {
            fileOperator.truncate(0);
            logIndexCache.clear();
            minIndex = 0;
            maxIndex = 0;
        } else if (index < maxIndex) {
            long position = (long) (index - minIndex + 1) * LogIndex.BYTES;
            fileOperator.truncate(position);
            logIndexCache.removeAfter(index);
            maxIndex = index;
        }
    }

    @Override
    public void exchangeLogFileMetadataRegion(LogIndexFileMetadataRegion logIndexFileMetadataRegion) {
        logIndexFileMetadataRegion.write(this.logIndexFileMetadataRegion.read());
        this.logIndexFileMetadataRegion.clear();
        this.logIndexFileMetadataRegion = logIndexFileMetadataRegion;
        fileOperator.changeFileHeaderOperator(logIndexFileMetadataRegion);
    }

    @Override
    public boolean isEmpty() {
        return fileOperator.isEmpty();
    }

    @Override
    public long size() {
        return fileOperator.size();
    }

    @Override
    public void close() {
        fileOperator.close();
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
