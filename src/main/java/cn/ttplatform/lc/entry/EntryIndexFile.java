package cn.ttplatform.lc.entry;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Wang Hao
 * @date 2020/7/1 下午10:13
 */
public class EntryIndexFile {

    private static final long ENTRY_INDEX_ITEM_LENGTH = 1L << 4;
    private static final long INDEX_LENGTH = Integer.BYTES;
    private static final EntryFactory ENTRYFACTORY = EntryFactory.INSTANCE;
    private final RandomAccessFile file;
    private final FileChannel channel;
    private int minEntryIndex;
    private int maxEntryIndex;
    private int count;
    private final Map<Integer, EntryIndex> entryIndexMap = new HashMap<>();

    public EntryIndexFile(RandomAccessFile file) {
        this.file = file;
        this.channel = this.file.getChannel();
        load();
    }

    private void load() {
        if (isEmpty()) {
            return;
        }
        try {
            minEntryIndex = file.readInt();
            maxEntryIndex = file.readInt();
            updateCount();
            MappedByteBuffer byteBuffer;
            long position = INDEX_LENGTH << 1;
            for (int index = minEntryIndex; index <= maxEntryIndex; ++index) {
                byteBuffer = channel.map(MapMode.READ_ONLY, position, ENTRY_INDEX_ITEM_LENGTH);
                EntryIndex entryIndex = ENTRYFACTORY
                    .createEntryIndex(byteBuffer.getLong(), byteBuffer.getInt(), byteBuffer.getInt());
                entryIndexMap.put(index, entryIndex);
                position += ENTRY_INDEX_ITEM_LENGTH;
            }
        } catch (IOException e) {
            throw new IllegalStateException("Read file error");
        }

    }

    public long getEntryOffset(int index) {
        EntryIndex entryIndex = entryIndexMap.get(index);
        return entryIndex != null ? entryIndex.getOffset() : -1L;
    }

    public void appendEntryIndex(int index, long offset, int type, int term) throws IOException {
        if (isEmpty()) {
            file.seek(0L);
            file.writeInt(0);
        } else {
            if (index != maxEntryIndex + 1) {
                throw new IllegalArgumentException(
                    "index(" + index + ") is not correct, maxEntryIndex is " + maxEntryIndex);
            }
            file.seek(INDEX_LENGTH);
        }
        maxEntryIndex = index;
        file.writeInt(maxEntryIndex);
        updateCount();
        long position = 2 * INDEX_LENGTH + (count - 1) * ENTRY_INDEX_ITEM_LENGTH;
        MappedByteBuffer byteBuffer = channel.map(MapMode.READ_WRITE, position, ENTRY_INDEX_ITEM_LENGTH);
        byteBuffer.putLong(offset);
        byteBuffer.putInt(type);
        byteBuffer.putInt(term);
    }

    public long size() {
        try {
            return file.length();
        } catch (IOException e) {
            throw new IllegalStateException("Get file length error");
        }
    }

    public void updateCount() {
        count = maxEntryIndex - minEntryIndex + 1;
    }

    public boolean isEmpty() {
        return size() == 0L;
    }
}
