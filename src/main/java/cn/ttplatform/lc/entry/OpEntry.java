package cn.ttplatform.lc.entry;

import java.nio.charset.Charset;

/**
 * @author Wang Hao
 * @date 2020/7/1 下午9:39
 */
public class OpEntry extends AbstractEntry {

    private final byte[] command;

    public OpEntry(int type, int term, int index, byte[] command) {
        super(type, term, index);
        this.command = command;
    }

    @Override
    public byte[] getCommand() {
        return command;
    }

    @Override
    public String toString() {
        return "OpEntry{type = " + this.getType() + ", term = " + this.getTerm() + ", index = " + this.getIndex()
            + ", command = " + new String(command, Charset.defaultCharset()) + '}';
    }
}
