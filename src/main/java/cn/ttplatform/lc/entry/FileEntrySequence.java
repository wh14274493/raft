package cn.ttplatform.lc.entry;

import java.util.LinkedList;
import java.util.List;

/**
 * @author Wang Hao
 * @date 2020/7/1 下午10:06
 */
public class FileEntrySequence extends AbstractEntrySequence {

    private final EntryFile entryFile;
    private final EntryIndexFile entryIndexFile;
    private final List<Entry> pendingList = new LinkedList<>();

    public FileEntrySequence(int entryIndexOffset, EntryFile entryFile, EntryIndexFile entryIndexFile) {
        super(entryIndexOffset);
        this.entryFile = entryFile;
        this.entryIndexFile = entryIndexFile;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public Entry getEntry(int index) {
        long offset = entryIndexFile.getEntryOffset(index);
        return offset != -1L ? entryFile.loadEntry(offset) : null;
    }
}
