package cn.ttplatform.lc.node.role;

import cn.ttplatform.lc.constant.RoleType;

import java.util.concurrent.ScheduledFuture;

/**
 * @author Wang Hao
 * @date 2020/6/30 下午9:14
 */
public class Follower extends AbstractRole {

    private String voteTo;
    private final ScheduledFuture<?> electionTimeoutFuture;

    public Follower(int term, String voteTo, ScheduledFuture<?> future) {
        super(term, RoleType.FOLLOWER);
        this.voteTo = voteTo;
        this.electionTimeoutFuture = future;
    }

    public String getVoteTo() {
        return voteTo;
    }

    @Override
    public void cancelTask() {
        this.electionTimeoutFuture.cancel(false);
    }

}
