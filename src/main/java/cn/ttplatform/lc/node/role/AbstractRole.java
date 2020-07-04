package cn.ttplatform.lc.node.role;

/**
 * @author Wang Hao
 * @date 2020/6/30 下午9:12
 */
public abstract class AbstractRole {

    private int term;

    public AbstractRole(int term) {
        this.term = term;
    }

    public abstract void cancelTask();
}
