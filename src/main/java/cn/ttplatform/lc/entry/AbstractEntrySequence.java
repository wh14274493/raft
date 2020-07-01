package cn.ttplatform.lc.entry;

/**
 * @author Wang Hao
 * @date 2020/7/1 下午10:01
 */
public abstract class AbstractEntrySequence implements EntrySequence {

    int entryIndexOffset;
    int nextEntryIndex;

    public AbstractEntrySequence(int entryIndexOffset) {
        this.entryIndexOffset = entryIndexOffset;
        this.nextEntryIndex = entryIndexOffset;
    }

    @Override
    public int getFirstEntryIndex() {
        if (isEmpty()) {
            throw new RuntimeException("");
        }
        return entryIndexOffset;
    }

    @Override
    public int getLastEntryIndex() {
        if (isEmpty()) {
            throw new RuntimeException("");
        }
        return nextEntryIndex - 1;
    }

}
