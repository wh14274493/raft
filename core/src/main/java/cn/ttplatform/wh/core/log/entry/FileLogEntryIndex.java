package cn.ttplatform.wh.core.log.entry;

import cn.ttplatform.wh.constant.FileName;
import cn.ttplatform.wh.domain.entry.LogEntryIndex;
import cn.ttplatform.wh.core.common.RandomAccessFileWrapper;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import lombok.Getter;

/**
 * @author Wang Hao
 * @date 2020/7/1 下午10:13
 */
@Getter
public class FileLogEntryIndex {

    private static final long HEADER_LENGTH = Integer.BYTES << 1;
    private static final long ITEM_LENGTH = Integer.BYTES * 3 + Long.BYTES;
    private static final long INDEX_LENGTH = Integer.BYTES;
    private final RandomAccessFileWrapper file;
    private int minLogIndex;
    private int maxLogIndex;
    private final Map<Integer, LogEntryIndex> entryIndexMap = new HashMap<>();

    public FileLogEntryIndex(File parent) {
        file = new RandomAccessFileWrapper(new File(parent, FileName.INDEX_FILE_NAME));
        if (!isEmpty()) {
            minLogIndex = file.readInt();
            maxLogIndex = file.readInt();
            for (int i = minLogIndex; i <= maxLogIndex; ++i) {
                long offset = file.readLong();
                int type = file.readInt();
                int term = file.readInt();
                int index = file.readInt();
                LogEntryIndex logEntryIndex = LogEntryIndex.builder().type(type).term(term).offset(offset).index(index)
                    .build();
                entryIndexMap.put(index, logEntryIndex);
            }
        }
    }

    public LogEntryIndex getLastEntryIndex() {
        return entryIndexMap.get(maxLogIndex);
    }

    public long getEntryOffset(int index) {
        LogEntryIndex logEntryIndex = getEntryMetaData(index);
        return logEntryIndex != null ? logEntryIndex.getOffset() : -1L;
    }

    public LogEntryIndex getEntryMetaData(int index) {
        return entryIndexMap.get(index);
    }

    public void append(int index, long offset, int type, int term) {
        if (isEmpty()) {
            minLogIndex = index;
            file.writeInt(minLogIndex);
        } else {
            if (index != maxLogIndex + 1) {
                throw new IllegalArgumentException(
                    "index[" + index + "] is not correct, maxEntryIndex is " + maxLogIndex);
            }
            file.seek(INDEX_LENGTH);
        }
        maxLogIndex = index;
        file.writeInt(maxLogIndex);
        file.seek(file.size());
        file.writeLong(offset);
        file.writeInt(type);
        file.writeInt(term);
        file.writeInt(index);
        entryIndexMap.put(index, LogEntryIndex.builder().index(index).term(term).offset(offset).type(type).build());
    }

    public void removeAfter(int index) {
        if (index < minLogIndex) {
            file.clear();
            entryIndexMap.clear();
            minLogIndex = 0;
            maxLogIndex = 0;
        } else if (index < maxLogIndex) {
            long position = HEADER_LENGTH + (index - minLogIndex + 1) * ITEM_LENGTH;
            file.truncate(position);
            file.seek(INDEX_LENGTH);
            maxLogIndex = index;
            file.writeInt(maxLogIndex);
            entryIndexMap.entrySet().removeIf(entry -> entry.getKey() > maxLogIndex);
        }
    }

    public boolean isEmpty() {
        return file.isEmpty();
    }

    public void close() {
        file.close();
    }
}
