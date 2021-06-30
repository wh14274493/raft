package cn.ttplatform.wh.data.log;

import cn.ttplatform.wh.config.ServerProperties;
import cn.ttplatform.wh.data.log.SyncLogIndexFile.LogIndexCache;
import cn.ttplatform.wh.data.support.AsyncFileOperator;
import cn.ttplatform.wh.exception.IncorrectLogIndexNumberException;
import cn.ttplatform.wh.support.Pool;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;

import static cn.ttplatform.wh.data.FileConstant.LOG_INDEX_FILE_HEADER_SIZE;

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

    public AsyncLogIndexFile(File file, ServerProperties properties, Pool<ByteBuffer> byteBufferPool, int lastIncludeIndex) {
        this.logIndexCache = new LogIndexCache(properties.getLogIndexCacheSize());
        this.fileOperator = new AsyncFileOperator(properties, byteBufferPool, file, LOG_INDEX_FILE_HEADER_SIZE);
        this.lastIncludeIndex = lastIncludeIndex;
        this.minIndex = lastIncludeIndex;
        this.maxIndex = lastIncludeIndex;
        initialize();
    }

    @Override
    public void initialize() {
        if (!isEmpty()) {
            long position = LOG_INDEX_FILE_HEADER_SIZE;
            LogIndex logIndex = loadLogIndex(position);
            this.minIndex = logIndex.getIndex();
            position = fileOperator.getSize() - LogIndex.BYTES;
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
            logIndex = loadLogIndex((long) (index - minIndex) * LogIndex.BYTES + LOG_INDEX_FILE_HEADER_SIZE);
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
        if (offset < LOG_INDEX_FILE_HEADER_SIZE) {
            throw new IllegalArgumentException(String.format("offset[%d] must greater than %d.", offset, LOG_INDEX_FILE_HEADER_SIZE));
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
        fileOperator.appendInt(logIndex.getIndex());
        fileOperator.appendInt(logIndex.getTerm());
        fileOperator.appendInt(logIndex.getType());
        fileOperator.appendLong(logIndex.getOffset());
        logIndexCache.put(logIndex.getIndex(), logIndex);
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
        for (int i = 0; i < logs.size(); i++) {
            LogIndex logIndex = logs.get(i).getMetadata();
            logIndex.setOffset(offsets[i]);
            fileOperator.appendInt(logIndex.getIndex());
            fileOperator.appendInt(logIndex.getTerm());
            fileOperator.appendInt(logIndex.getType());
            fileOperator.appendLong(logIndex.getOffset());
            logIndexCache.put(logIndex.getIndex(), logIndex);
        }
    }

    @Override
    public void append(ByteBuffer byteBuffer) {
        fileOperator.appendBytes(byteBuffer);
    }

    @Override
    public void removeAfter(int index) {
        if (index < minIndex) {
            fileOperator.truncate(LOG_INDEX_FILE_HEADER_SIZE);
            logIndexCache.clear();
            minIndex = lastIncludeIndex;
            maxIndex = lastIncludeIndex;
        } else if (index < maxIndex) {
            long position = (long) (index - minIndex + 1) * LogIndex.BYTES + LOG_INDEX_FILE_HEADER_SIZE;
            fileOperator.truncate(position);
            logIndexCache.removeAfter(index);
            maxIndex = index;
        }
    }

    @Override
    public long size() {
        return fileOperator.getSize();
    }

    @Override
    public boolean isEmpty() {
        return fileOperator.isEmpty();
    }

    @Override
    public void close() {
        fileOperator.close();
    }
}
