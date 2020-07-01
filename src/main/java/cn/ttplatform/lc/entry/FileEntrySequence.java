package cn.ttplatform.lc.entry;

/**
 * @author Wang Hao
 * @date 2020/7/1 下午10:06
 */
public class FileEntrySequence extends AbstractEntrySequence {

    private final EntryFile entryFile;
    private final EntryIndexFile entryIndexFile;

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
        return null;
    }
}
