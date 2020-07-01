package cn.ttplatform.lc.entry;

import lombok.Getter;

/**
 * @author Wang Hao
 * @date 2020/7/1 下午9:29
 */
public abstract class AbstractEntry implements Entry{

    private final int type;
    private final int term;
    private final int index;

    public AbstractEntry(int type, int term, int index) {
        this.type = type;
        this.term = term;
        this.index = index;
    }

    @Override
    public int getIndex() {
        return index;
    }

    @Override
    public int getTerm() {
        return term;
    }

    @Override
    public int getType() {
        return type;
    }
}
