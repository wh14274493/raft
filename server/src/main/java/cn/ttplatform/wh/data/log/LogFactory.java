package cn.ttplatform.wh.data.log;

import cn.ttplatform.wh.data.index.LogIndex;

/**
 * @author Wang Hao
 * @date 2020/7/1 下午10:13
 */
public class LogFactory {

    private LogFactory() {
    }

    public static Log createEntry(int type, int term, int index, byte[] command) {
        switch (type) {
            case Log.NO_OP_TYPE:
                return NoOpLog.builder().metadata(LogIndex.builder().type(type).term(term).index(index).build())
                    .build();
            case Log.NEW:
            case Log.OLD_NEW:
            case Log.SET:
                return OpLog.builder().metadata(LogIndex.builder().type(type).term(term).index(index).build())
                    .command(command).build();
            default:
                throw new IllegalArgumentException("unknown log type [" + type + "]");
        }
    }

}
