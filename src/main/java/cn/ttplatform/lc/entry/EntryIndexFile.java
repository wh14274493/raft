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

    private static final int ENTRY_INDEX_ITEM_LENGTH = 1 << 4;
    private static final int INDEX_LENGTH = Integer.BYTES;
    private final RandomAccessFile file;
    private final FileChannel channel;
    private int minEntryIndex;
    private int maxEntryIndex;
    private int count;
    private final Map<Integer, EntryIndex> entryIndexMap = new HashMap<>();

    public EntryIndexFile(RandomAccessFile file) throws IOException {
        this.file = file;
        this.channel = this.file.getChannel();
        load();
    }

    private void load() throws IOException {
        if (size() == 0) {
            return;
        }
        minEntryIndex = file.readInt();
        maxEntryIndex = file.readInt();
        updateCount();
        MappedByteBuffer byteBuffer;
        for (int index = minEntryIndex, position = INDEX_LENGTH << 1; index <= maxEntryIndex; ++index) {
            byteBuffer = channel.map(MapMode.READ_ONLY, position, ENTRY_INDEX_ITEM_LENGTH);
            EntryIndex entryIndex = EntryFactory
                .createEntryIndex(byteBuffer.getLong(), byteBuffer.getInt(), byteBuffer.getInt());
            entryIndexMap.put(index, entryIndex);
            position += ENTRY_INDEX_ITEM_LENGTH;
        }
    }

    public void appendEntryIndex(int index, long offset, int type, int term) throws IOException {
        if (size() == 0) {
            file.seek(0L);
            file.writeInt(0);
        } else {
            if (index != maxEntryIndex + 1) {
                throw new IllegalArgumentException(
                    "index[" + index + "] is not correct, maxEntryIndex is " + maxEntryIndex);
            }
            file.seek(INDEX_LENGTH);
        }
        maxEntryIndex = index;
        file.writeInt(maxEntryIndex);
        updateCount();
        int position = 2 * INDEX_LENGTH + (count - 1) * ENTRY_INDEX_ITEM_LENGTH;
        MappedByteBuffer byteBuffer = channel.map(MapMode.READ_WRITE, position, ENTRY_INDEX_ITEM_LENGTH);
        byteBuffer.putLong(offset);
        byteBuffer.putInt(type);
        byteBuffer.putInt(term);
    }

    public long size() throws IOException {
        return file.length();
    }

    public void updateCount() {
        count = maxEntryIndex - minEntryIndex + 1;
    }
}
