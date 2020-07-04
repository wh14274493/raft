package cn.ttplatform.lc.entry;

/**
 * @author Wang Hao
 * @date 2020/7/1 下午10:13
 */
public class EntryFactory {

    public static final EntryFactory INSTANCE = new EntryFactory();

    private EntryFactory() {
    }

    public Entry createEntry(int type, int term, int index, byte[] command) {
        switch (type) {
            case Entry.NO_OP_TYPE:
                return new NoOpEntry(type, term, index);
            case Entry.OP_TYPE:
                return new OpEntry(type, term, index, command);
            default:
                throw new IllegalArgumentException("unknown entry type [" + type + "]");
        }
    }

    public EntryIndex createEntryIndex(long offset, int type, int term) {
        return new EntryIndex(offset, type, term);
    }
}
