package cn.ttplatform.lc.entry;

/**
 * @author Wang Hao
 * @date 2020/7/1 下午9:37
 */
public class NoOpEntry extends AbstractEntry {

    public NoOpEntry(int type, int term, int index) {
        super(type, term, index);
    }

    @Override
    public String toString() {
        return "OpEntry{type = " + this.getType() + ", term = " + this.getTerm() + ", index = " + this.getIndex() + "}";
    }

    @Override
    public byte[] getCommand() {
        return new byte[0];
    }
}
