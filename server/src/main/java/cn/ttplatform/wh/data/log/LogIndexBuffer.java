package cn.ttplatform.wh.data.log;

import cn.ttplatform.wh.GlobalContext;
import cn.ttplatform.wh.data.FileConstant;
import cn.ttplatform.wh.data.log.LogIndexFile.LogIndexCache;
import cn.ttplatform.wh.data.pool.BlockCache;
import cn.ttplatform.wh.exception.IncorrectLogIndexNumberException;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Wang Hao
 * @date 2021/6/11 22:45
 */
@Slf4j
public class LogIndexBuffer implements LogIndexOperation {

    /**
     * index(4 bytes) + term(4 bytes) + type(4 bytes) + offset(8 bytes) = 20
     */
    private static final int ITEM_LENGTH = Integer.BYTES * 3 + Long.BYTES;
    private int minIndex;
    private int maxIndex;
    BlockCache blockCache;
    private final LogIndexCache logIndexCache;

    public LogIndexBuffer(File file, GlobalContext context) {
        this.logIndexCache = new LogIndexCache(100);
        this.blockCache = new BlockCache(context, file, FileConstant.LOG_INDEX_FILE_HEADER_SIZE);
    }

    @Override
    public void initialize() {
        if (!isEmpty()) {
            long position = 0L;
            LogIndex logIndex = loadLogIndex(position);
            this.minIndex = logIndex.getIndex();
            logIndexCache.put(minIndex, logIndex);
            position = blockCache.getSize() - ITEM_LENGTH;
            logIndex = loadLogIndex(position);
            this.maxIndex = logIndex.getIndex();
            logIndexCache.put(maxIndex, logIndex);
        }
        log.info("initialize minIndex = {}, maxIndex = {}.", minIndex, maxIndex);
    }

    private LogIndex loadLogIndex(long position) {
        int index = blockCache.getInt(position);
        position += 4;
        int term = blockCache.getInt(position);
        position += 4;
        int type = blockCache.getInt(position);
        position += 4;
        long offset = blockCache.getLong(position);
        this.minIndex = index;
        return LogIndex.builder().index(index).term(term).type(type).offset(offset).build();
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
        return Optional.ofNullable(logIndexCache.get(index))
            .orElse(loadLogIndex((long) (index - minIndex) * ITEM_LENGTH));
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
            long position = (long) (index - minIndex + 1) * ITEM_LENGTH;
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
        return blockCache.getSize() == 0L;
    }

    @Override
    public void close() {
        blockCache.close();
    }
}
