package cn.ttplatform.wh.data.log;

import cn.ttplatform.wh.GlobalContext;
import cn.ttplatform.wh.config.ServerProperties;
import cn.ttplatform.wh.data.FileConstant;
import cn.ttplatform.wh.data.log.LogIndexFile.LogIndexCache;
import cn.ttplatform.wh.data.pool.BlockCache;
import cn.ttplatform.wh.exception.IncorrectLogIndexNumberException;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * @author Wang Hao
 * @date 2021/6/11 22:45
 */
@Slf4j
public class LogIndexBuffer implements LogIndexOperation {

    private int minIndex;
    private int maxIndex;
    private final BlockCache blockCache;
    private final LogIndexCache logIndexCache;

    public LogIndexBuffer(File file, ServerProperties properties) {
        this.logIndexCache = new LogIndexCache(properties.getLogIndexCacheSize());
        this.blockCache = new BlockCache(properties.getBlockCacheSize(), properties.getBlockSize(),
                properties.getBlockFlushInterval(), file, FileConstant.LOG_INDEX_FILE_HEADER_SIZE);
        initialize();
    }

    @Override
    public void initialize() {
        if (!isEmpty()) {
            long position = FileConstant.LOG_INDEX_FILE_HEADER_SIZE;
            LogIndex logIndex = loadLogIndex(position);
            this.minIndex = logIndex.getIndex();
            position = blockCache.getSize() - LogIndex.BYTES;
            logIndex = loadLogIndex(position);
            this.maxIndex = logIndex.getIndex();
        }
        log.info("initialize minIndex = {}, maxIndex = {}.", minIndex, maxIndex);
    }

    private LogIndex loadLogIndex(long position) {
        int index = blockCache.getInt(position);
        position += Integer.BYTES;
        int term = blockCache.getInt(position);
        position += Integer.BYTES;
        int type = blockCache.getInt(position);
        position += Integer.BYTES;
        long offset = blockCache.getLong(position);
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
            logIndex = loadLogIndex((long) (index - minIndex) * LogIndex.BYTES + FileConstant.LOG_INDEX_FILE_HEADER_SIZE);
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
        if (offset < FileConstant.LOG_INDEX_FILE_HEADER_SIZE) {
            throw new IllegalArgumentException("offset[" + offset + "] must greater than " + FileConstant.LOG_INDEX_FILE_HEADER_SIZE + ".");
        }
        int index = log.getIndex();
        if (isEmpty()) {
            minIndex = index;
        } else {
            if (index != maxIndex + 1) {
                throw new IncorrectLogIndexNumberException(
                        "index[" + index + "] is not correct, maxEntryIndex is " + maxIndex);
            }
        }
        maxIndex = index;
        LogIndexBuffer.log.debug("update maxLogIndex = {}.", maxIndex);
        LogIndex logIndex = LogIndex.builder().index(index).term(log.getTerm()).offset(offset).type(log.getType())
                .build();
        blockCache.appendInt(log.getIndex());
        blockCache.appendInt(log.getTerm());
        blockCache.appendInt(log.getType());
        blockCache.appendLong(logIndex.getOffset());
        logIndexCache.put(logIndex.getIndex(), logIndex);
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
        for (int i = 0; i < logs.size(); i++) {
            Log log = logs.get(i);
            blockCache.appendInt(log.getIndex());
            blockCache.appendInt(log.getTerm());
            blockCache.appendInt(log.getType());
            blockCache.appendLong(offsets[i]);
            LogIndex logIndex = LogIndex.builder()
                    .index(log.getIndex())
                    .term(log.getTerm())
                    .type(log.getType())
                    .offset(offsets[i])
                    .build();
            logIndexCache.put(logIndex.getIndex(), logIndex);
        }
    }

    @Override
    public void append(ByteBuffer byteBuffer) {
        blockCache.appendByteBuffer(byteBuffer);
    }

    @Override
    public void removeAfter(int index) {
        if (index < minIndex) {
            blockCache.removeAfter(0L);
            logIndexCache.clear();
            minIndex = 0;
            maxIndex = 0;
        } else if (index < maxIndex) {
            long position = (long) (index - minIndex + 1) * LogIndex.BYTES;
            blockCache.removeAfter(position);
            maxIndex = index;
            logIndexCache.removeAfter(index);
        }
    }

    @Override
    public long size() {
        return blockCache.getSize();
    }

    @Override
    public boolean isEmpty() {
        return blockCache.getSize() <= FileConstant.LOG_INDEX_FILE_HEADER_SIZE;
    }

    @Override
    public void close() {
        blockCache.close();
    }
}