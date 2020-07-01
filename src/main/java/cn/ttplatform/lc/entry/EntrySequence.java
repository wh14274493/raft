package cn.ttplatform.lc.entry;

/**
 * @author Wang Hao
 * @date 2020/7/1 下午9:58
 */
public interface EntrySequence {

    boolean isEmpty();

    int getFirstEntryIndex();

    int getLastEntryIndex();

    Entry getEntry(int index);
}
