package cn.ttplatform.lc.role;

import java.util.concurrent.ScheduledFuture;

/**
 * @author Wang Hao
 * @date 2020/6/30 下午9:16
 */
public class Candidate extends AbstractRole {

    private int voteCounts;
    private final ScheduledFuture<?> electionTimeoutFuture;

    public Candidate(int term, ScheduledFuture<?> future) {
        this(term, 0, future);
    }

    public Candidate(int term, int voteCounts, ScheduledFuture<?> future) {
        super(term);
        this.voteCounts = voteCounts;
        this.electionTimeoutFuture = future;
    }

    @Override
    public void cancelTask() {
        this.electionTimeoutFuture.cancel(false);
    }
}
