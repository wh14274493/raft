package cn.ttplatform.wh.core.log.entry;

/**
 * @author Wang Hao
 * @date 2020/7/1 下午10:13
 */
public class LogEntryFactory {

    private static class LazyHolder {

        private static final LogEntryFactory INSTANCE = new LogEntryFactory();
    }

    public static LogEntryFactory getInstance() {
        return LazyHolder.INSTANCE;
    }

    private LogEntryFactory() {
    }

    public static LogEntry createEntry(int type, int term, int index, byte[] command, int cmdLength) {
        switch (type) {
            case LogEntry.NO_OP_TYPE:
                return NoOpLogEntry.builder().metadata(LogEntryIndex.builder().type(type).term(term).index(index).build())
                    .build();
            case LogEntry.NEW:
            case LogEntry.OLD_NEW:
            case LogEntry.SET:
                return OpLogEntry.builder().metadata(LogEntryIndex.builder().type(type).term(term).index(index).build())
                    .command(command).commandLength(cmdLength).build();
            default:
                throw new IllegalArgumentException("unknown entry type [" + type + "]");
        }
    }

}
