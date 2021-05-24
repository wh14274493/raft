package cn.ttplatform.wh.core.data.log;

/**
 * @author Wang Hao
 * @date 2020/7/1 下午10:22
 */
public interface Log {

    int NO_OP_TYPE = 0;
    int SET = 1;
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

    void setIndex(int index);

    byte[] getCommand();

    int getCommandLength();

    LogIndex getMetadata();

}
