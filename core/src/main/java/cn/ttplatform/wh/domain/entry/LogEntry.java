package cn.ttplatform.wh.domain.entry;

/**
 * @author Wang Hao
 * @date 2020/7/1 下午10:22
 */
public interface LogEntry {

    int NO_OP_TYPE = 0;
    int OP_TYPE = 1;

    /**
     * Get type for entry
     * @return entry type
     */
    int getType();

    /**
     * Get term for entry
     * @return term
     */
    int getTerm();

    int getIndex();

    byte[] getCommand();

}
