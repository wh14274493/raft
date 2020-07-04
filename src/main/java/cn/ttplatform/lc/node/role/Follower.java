package cn.ttplatform.lc.node.role;

import java.util.concurrent.ScheduledFuture;

/**
 * @author Wang Hao
 * @date 2020/6/30 下午9:14
 */
public class Follower extends AbstractRole {

    private String voteTo;
    private final ScheduledFuture<?> electionTimeoutFuture;

    public Follower(int term, ScheduledFuture<?> future) {
        super(term);
        this.electionTimeoutFuture = future;
    }

    @Override
    public void cancelTask() {
        this.electionTimeoutFuture.cancel(false);
    }

}
