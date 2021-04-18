package cn.ttplatform.wh.core.log;

import cn.ttplatform.wh.domain.entry.LogEntry;
import cn.ttplatform.wh.domain.entry.NoOpLogEntry;
import cn.ttplatform.wh.domain.entry.OpLogEntry;

/**
 * @author Wang Hao
 * @date 2020/7/1 下午10:13
 */
public class EntryFactory {

    public static final EntryFactory INSTANCE = new EntryFactory();

    private EntryFactory() {
    }

    public LogEntry createEntry(int type, int term, int index, byte[] command) {
        switch (type) {
            case LogEntry.NO_OP_TYPE:
                return NoOpLogEntry.builder().type(LogEntry.NO_OP_TYPE).term(term).index(index).build();
            case LogEntry.OP_TYPE:
                return OpLogEntry.builder().type(LogEntry.OP_TYPE).term(term).index(index).command(command).build();
            default:
                throw new IllegalArgumentException("unknown entry type [" + type + "]");
        }
    }

}
