package cn.ttplatform.wh.core.log.entry;

/**
 * @author Wang Hao
 * @date 2020/7/1 下午10:22
 */
public interface LogEntry {

    int NO_OP_TYPE = 0;
    int OP_TYPE = 1;
    int OLD_NEW = 2;
    int NEW = 3;

    /**
     * Get type for entry
     *
     * @return entry type
     */
    int getType();

    /**
     * Get term for entry
     *
     * @return term
     */
    int getTerm();

    /**
     * Get inde for entry
     *
     * @return log index
     */
    int getIndex();

    byte[] getCommand();

    LogEntryIndex getMetadata();

}
