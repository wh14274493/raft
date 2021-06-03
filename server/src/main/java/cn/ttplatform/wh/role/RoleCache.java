package cn.ttplatform.wh.role;

/**
 * @author Wang Hao
 * @date 2021/5/3 11:31
 */
public class RoleCache {

    private Follower follower;
    private Leader leader;
    private Candidate candidate;

    public void recycle(Role role) {
        switch (role.getType()) {
            case LEADER:
                recycleLeader((Leader) role);
                break;
            case CANDIDATE:
                recycleCandidate((Candidate) role);
                break;
            default:
                recycleFollower((Follower) role);
        }
    }

    public Candidate getCandidate() {
        if (candidate == null) {
            candidate = new Candidate();
        }
        return candidate;
    }

    private void recycleCandidate(Candidate candidate) {
        candidate.cancelTask();
        candidate.setTerm(0);
        candidate.setScheduledFuture(null);
        candidate.setVoteCounts(0);
        this.candidate = candidate;
    }

    public Follower getFollower() {
        if (follower == null) {
            follower = new Follower();
        }
        return follower;
    }

    private void recycleFollower(Follower follower) {
        follower.cancelTask();
        follower.setTerm(0);
        follower.setScheduledFuture(null);
        follower.setLeaderId(null);
        follower.setPreVoteCounts(0);
        follower.setVoteTo(null);
        follower.setLastHeartBeat(0L);
    }

    public Leader getLeader() {
        if (leader == null) {
            leader = new Leader();
        }
        return leader;
    }

    private void recycleLeader(Leader leader) {
        leader.cancelTask();
        leader.setTerm(0);
    }
}
