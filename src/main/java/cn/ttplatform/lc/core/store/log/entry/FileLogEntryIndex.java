package cn.ttplatform.lc.core.store.log.entry;

import cn.ttplatform.lc.core.store.RandomAccessFileWrapper;
import java.io.File;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Wang Hao
 * @date 2020/7/1 下午10:13
 */
public class FileLogEntryIndex {

    private static final long INDEX_LENGTH = Integer.BYTES;
    private final RandomAccessFileWrapper file;
    private int minLogIndex;
    private int maxLogIndex;
    private int count;
    private final Map<Integer, LogEntryIndex> entryIndexMap = new HashMap<>();

    public FileLogEntryIndex(File file) {
        this.file = new RandomAccessFileWrapper(file);
        load();
    }

    private void load() {
        if (isEmpty()) {
            return;
        }
        minLogIndex = file.readInt();
        maxLogIndex = file.readInt();
        updateCount();
        for (int i = minLogIndex; i <= maxLogIndex; ++i) {
            long offset = file.readLong();
            int type = file.readInt();
            int term = file.readInt();
            int index = file.readInt();
            LogEntryIndex logEntryIndex = LogEntryIndex.builder().type(type).term(term).offset(offset).index(index).build();
            entryIndexMap.put(index, logEntryIndex);
        }
    }

    public int getMinLogIndex() {
        return minLogIndex;
    }

    public int getMaxLogIndex() {
        return maxLogIndex;
    }

    public LogEntryIndex getLastEntryIndex() {
        return entryIndexMap.get(maxLogIndex);
    }

    public long getEntryOffset(int index) {
        LogEntryIndex logEntryIndex = entryIndexMap.get(index);
        return logEntryIndex != null ? logEntryIndex.getOffset() : -1L;
    }

    public void append(int index, long offset, int type, int term) {
        if (isEmpty()) {
            minLogIndex = index;
            file.writeInt(minLogIndex);
        } else {
            if (index != maxLogIndex + 1) {
                throw new IllegalArgumentException(
                    "index(" + index + ") is not correct, maxEntryIndex is " + maxLogIndex);
            }
            file.seek(INDEX_LENGTH);
        }
        maxLogIndex = index;
        file.writeInt(maxLogIndex);
        updateCount();
        file.seek(file.size());
        file.writeLong(offset);
        file.writeInt(type);
        file.writeInt(term);
    }

    public void updateCount() {
        count = maxLogIndex - minLogIndex + 1;
    }

    public boolean isEmpty() {
        return file.size() == 0;
    }

    public void close(){
        file.close();
    }
}
