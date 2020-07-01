package cn.ttplatform.lc.entry;

/**
 * @author Wang Hao
 * @date 2020/7/1 下午10:22
 */
public interface Entry {

    int NO_OP_TYPE = 0;
    int OP_TYPE = 1;

    int getType();

    int getTerm();

    int getIndex();

    byte[] getCommand();

}
