package cn.ttplatform.wh.core.data.log;

/**
 * @author Wang Hao
 * @date 2020/7/1 下午10:13
 */
public class LogFactory {

    private static class LazyHolder {

        private static final LogFactory INSTANCE = new LogFactory();
    }

    public static LogFactory getInstance() {
        return LazyHolder.INSTANCE;
    }

    private LogFactory() {
    }

    public static Log createEntry(int type, int term, int index, byte[] command, int cmdLength) {
        switch (type) {
            case Log.NO_OP_TYPE:
                return NoOpLog.builder().metadata(LogIndex.builder().type(type).term(term).index(index).build())
                    .build();
            case Log.NEW:
            case Log.OLD_NEW:
            case Log.SET:
                return OpLog.builder().metadata(LogIndex.builder().type(type).term(term).index(index).build())
                    .command(command).commandLength(cmdLength).build();
            default:
                throw new IllegalArgumentException("unknown entry type [" + type + "]");
        }
    }

}
