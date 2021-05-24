package cn.ttplatform.wh.core.data.log;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.DSYNC;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

import cn.ttplatform.wh.core.data.tool.ByteBufferWriter;
import cn.ttplatform.wh.core.data.tool.PooledByteBuffer;
import cn.ttplatform.wh.core.data.tool.ReadableAndWriteableFile;
import cn.ttplatform.wh.exception.IncorrectLogIndexNumberException;
import cn.ttplatform.wh.support.Pool;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Wang Hao
 * @date 2020/7/1 下午10:13
 */
@Slf4j
@Getter
public final class LogIndexFile {

    private static final int ITEM_LENGTH = Integer.BYTES * 3 + Long.BYTES;
    private static final long INDEX_LENGTH = Integer.BYTES;
    private final List<LogIndex> logIndices;
    private final Pool<PooledByteBuffer> byteBufferPool;
    private final ReadableAndWriteableFile file;
    private int minIndex;
    private int maxIndex;

    public LogIndexFile(File file, Pool<PooledByteBuffer> byteBufferPool,int lastIncludeIndex) {
        this.file = new ByteBufferWriter(file, byteBufferPool);
        this.byteBufferPool = byteBufferPool;
        this.minIndex = lastIncludeIndex;
        this.maxIndex = lastIncludeIndex;
        if (!isEmpty()) {
            PooledByteBuffer content = null;
            try {
                content = this.file.readByteBufferAt(0L, (int) this.file.size());
                logIndices = new ArrayList<>(content.limit() / ITEM_LENGTH * 2);
                content.flip();
                ByteBuffer buffer = content.getBuffer();
                long base = -1;
                int position;
                int index;
                int term;
                int type;
                long offset;
                while (buffer.hasRemaining()) {
                    index = content.getInt();
                    term = content.getInt();
                    type = content.getInt();
                    position = buffer.position();
                    offset = buffer.getLong();
                    if (base == -1) {
                        base = offset;
                    }
                    if (base == 0) {
                        logIndices.add(LogIndex.builder().index(index).term(term).type(type).offset(offset).build());
                    } else {
                        offset -= base;
                        logIndices.add(LogIndex.builder().index(index).term(term).type(type).offset(offset).build());
                        content.position(position);
                        content.putLong(offset);
                    }
                }
                if (!logIndices.isEmpty()) {
                    minIndex = logIndices.get(0).getIndex();
                    maxIndex = logIndices.get(logIndices.size() - 1).getIndex();
                }
                if (base != 0) {
                    this.file.writeBytesAt(0L, content);
                }
            } finally {
                byteBufferPool.recycle(content);
            }
        } else {
            logIndices = new ArrayList<>();
        }
        log.info("initialize minIndex = {}, maxIndex = {}.", minIndex, maxIndex);
    }

    public LogIndex getLastLogMetaData() {
        return logIndices.get(logIndices.size() - 1);
    }

    public LogIndex getLogMetaData(int index) {
        if (index < minIndex || index > maxIndex) {
            return null;
        }
        return logIndices.get(index - minIndex);
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
                throw new IncorrectLogIndexNumberException(
                    "index[" + index + "] is not correct, maxEntryIndex is " + maxIndex);
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
            logIndices.add(logIndex);
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
                logIndices.add(LogIndex.builder()
                    .index(log.getIndex())
                    .term(log.getTerm())
                    .type(log.getType())
                    .offset(offsets[i])
                    .build());
            }
            file.append(byteBuffer, contentLength);
        } finally {
            byteBufferPool.recycle(byteBuffer);
        }
    }

    public void removeAfter(int index) {
        if (index < minIndex) {
            file.clear();
            logIndices.clear();
            minIndex = 0;
            maxIndex = 0;
        } else if (index < maxIndex) {
            long position = (long) (index - minIndex + 1) * ITEM_LENGTH;
            file.truncate(position);
            maxIndex = index;
//            TreeMap map = new TreeMap();
            logIndices.subList(index - minIndex + 1, logIndices.size()).clear();
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

    public void close() {
        file.close();
    }
}
