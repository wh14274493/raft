package cn.ttplatform.lc.entry;

import lombok.Getter;

/**
 * @author Wang Hao
 * @date 2020/7/1 下午10:54
 */
@Getter
public class EntryIndex {

    private final long offset;
    private final int type;
    private final int term;

    public EntryIndex(long offset, int type, int term) {
        this.offset = offset;
        this.term = term;
        this.type = type;
    }
}
