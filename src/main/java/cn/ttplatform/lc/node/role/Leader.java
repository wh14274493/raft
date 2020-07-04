package cn.ttplatform.lc.node.role;

import java.util.concurrent.ScheduledFuture;

/**
 * @author Wang Hao
 * @date 2020/6/30 下午9:15
 */
public class Leader extends AbstractRole {

    private final ScheduledFuture<?> logReplicationFuture;

    public Leader(int term, ScheduledFuture<?> future) {
        super(term);
        this.logReplicationFuture = future;
    }

    @Override
    public void cancelTask() {
        this.logReplicationFuture.cancel(false);
    }
}
