package cn.ttplatform.wh.data.index;

import cn.ttplatform.wh.config.ServerProperties;
import cn.ttplatform.wh.data.index.SyncLogIndexFile.LogIndexCache;
import cn.ttplatform.wh.data.log.Log;
import cn.ttplatform.wh.data.support.AsyncFileOperator;
import cn.ttplatform.wh.exception.IncorrectLogIndexNumberException;
import cn.ttplatform.wh.support.Pool;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;


/**
 * @author Wang Hao
 * @date 2021/6/11 22:45
 */
@Slf4j
public class AsyncLogIndexFile implements LogIndexOperation {

    private int minIndex;
    private int maxIndex;
    private final int lastIncludeIndex;
    private final AsyncFileOperator fileOperator;
    private final LogIndexCache logIndexCache;
    private LogIndexFileMetadataRegion logIndexFileMetadataRegion;

    public AsyncLogIndexFile(File file, ServerProperties properties, Pool<ByteBuffer> byteBufferPool, int lastIncludeIndex, LogIndexFileMetadataRegion logIndexFileMetadataRegion) {
        this.logIndexFileMetadataRegion = logIndexFileMetadataRegion;
        this.logIndexCache = new LogIndexCache(properties.getLogIndexCacheSize());
        this.fileOperator = new AsyncFileOperator(properties, byteBufferPool, file);
        this.lastIncludeIndex = lastIncludeIndex;
        this.minIndex = lastIncludeIndex;
        this.maxIndex = lastIncludeIndex;
        initialize();
    }

    @Override
    public void initialize() {
        if (!isEmpty()) {
            long position = 0;
            LogIndex logIndex = loadLogIndex(position);
            this.minIndex = logIndex.getIndex();
            position = logIndexFileMetadataRegion.getFileSize() - LogIndex.BYTES;
            logIndex = loadLogIndex(position);
            this.maxIndex = logIndex.getIndex();
        }
        log.info("initialize minIndex = {}, maxIndex = {}.", minIndex, maxIndex);
    }

    private LogIndex loadLogIndex(long position) {
        int index = fileOperator.getInt(position);
        position += Integer.BYTES;
        int term = fileOperator.getInt(position);
        position += Integer.BYTES;
        int type = fileOperator.getInt(position);
        position += Integer.BYTES;
        long offset = fileOperator.getLong(position);
        LogIndex logIndex = LogIndex.builder().index(index).term(term).type(type).offset(offset).build();
        logIndexCache.put(index, logIndex);
        return logIndex;
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
        if (index > maxIndex) {
            return null;
        }
        LogIndex logIndex = logIndexCache.get(index);
        if (logIndex == null) {
            logIndex = loadLogIndex((long) (index - minIndex) * LogIndex.BYTES);
        }
        return logIndex;
    }

    @Override
    public long getLogOffset(int index) {
        LogIndex logIndex = getLogMetaData(index);
        return logIndex != null ? logIndex.getOffset() : -1L;
    }

    @Override
    public void append(Log log, long offset) {
        if (offset < 0) {
            throw new IllegalArgumentException(String.format("offset[%d] must greater than %d.", offset, 0));
        }
        int index = log.getIndex();
        if (isEmpty()) {
            minIndex = index;
        } else {
            if (index != maxIndex + 1) {
                throw new IncorrectLogIndexNumberException(String.format("index[%d] is not correct, maxEntryIndex is %d.", index, maxIndex));
            }
        }
        maxIndex = index;
        AsyncLogIndexFile.log.debug("update maxLogIndex to {}.", maxIndex);
        LogIndex logIndex = log.getMetadata();
        logIndex.setOffset(offset);
        long fileSize = logIndexFileMetadataRegion.getFileSize();
        fileSize = append0(fileSize, logIndex);
        logIndexFileMetadataRegion.recordFileSize(fileSize);
    }

    private long append0(long position, LogIndex logIndex) {
        fileOperator.appendInt(position, logIndex.getIndex());
        position += 4;
        fileOperator.appendInt(position, logIndex.getTerm());
        position += 4;
        fileOperator.appendInt(position, logIndex.getType());
        position += 4;
        fileOperator.appendLong(position, logIndex.getOffset());
        position += 8;
        logIndexCache.put(logIndex.getIndex(), logIndex);
        return position;
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
        long fileSize = logIndexFileMetadataRegion.getFileSize();
        for (int i = 0; i < logs.size(); i++) {
            LogIndex logIndex = logs.get(i).getMetadata();
            logIndex.setOffset(offsets[i]);
            fileSize = append0(fileSize, logIndex);
        }
        logIndexFileMetadataRegion.recordFileSize(fileSize);
    }

    @Override
    public void append(ByteBuffer byteBuffer) {
        long fileSize = logIndexFileMetadataRegion.getFileSize();
        int limit = byteBuffer.limit();
        fileOperator.appendBytes(fileSize, byteBuffer);
        logIndexFileMetadataRegion.recordFileSize(fileSize + limit);
    }

    @Override
    public void removeAfter(int index) {
        long fileSize = logIndexFileMetadataRegion.getFileSize();
        if (index < minIndex) {
            fileOperator.truncate(0, fileSize);
            logIndexCache.clear();
            minIndex = lastIncludeIndex;
            maxIndex = lastIncludeIndex;
            logIndexFileMetadataRegion.recordFileSize(0);
        } else if (index < maxIndex) {
            long position = (long) (index - minIndex + 1) * LogIndex.BYTES;
            fileOperator.truncate(position, fileSize);
            logIndexCache.removeAfter(index);
            maxIndex = index;
            logIndexFileMetadataRegion.recordFileSize(position);
        }
    }

    @Override
    public void exchangeLogFileMetadataRegion(LogIndexFileMetadataRegion logIndexFileMetadataRegion) {
        logIndexFileMetadataRegion.recordFileSize(this.logIndexFileMetadataRegion.getFileSize());
        this.logIndexFileMetadataRegion.clear();
        this.logIndexFileMetadataRegion = logIndexFileMetadataRegion;
    }

    @Override
    public long size() {
        return logIndexFileMetadataRegion.getFileSize();
    }

    @Override
    public boolean isEmpty() {
        return logIndexFileMetadataRegion.getFileSize() <= 0;
    }

    @Override
    public void close() {
        fileOperator.close();
    }
}
