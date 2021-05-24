package cn.ttplatform.wh.core;

import cn.ttplatform.wh.config.RunMode;
import cn.ttplatform.wh.config.ServerProperties;
import cn.ttplatform.wh.core.connector.nio.NioConnector;
import cn.ttplatform.wh.core.executor.SingleThreadScheduler;
import cn.ttplatform.wh.core.group.Cluster;
import cn.ttplatform.wh.core.listener.Listener;
import cn.ttplatform.wh.core.listener.nio.NioListener;
import cn.ttplatform.wh.core.data.log.Log;
import cn.ttplatform.wh.core.role.Candidate;
import cn.ttplatform.wh.core.role.Follower;
import cn.ttplatform.wh.core.role.Leader;
import cn.ttplatform.wh.core.role.Role;
import cn.ttplatform.wh.core.role.RoleCache;
import cn.ttplatform.wh.core.role.RoleType;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * @author : wang hao
 * @date :  2020/8/15 23:16
 **/
@Getter
@Setter
@Slf4j
public class Node {

    private String selfId;
    private Role role;
    private RunMode mode;
    private final RoleCache roleCache;
    private final ServerProperties properties;
    private final GlobalContext context;
    private final NodeState nodeState;
    private boolean start;
    private boolean stop;
    private final Listener listener;

    public Node(ServerProperties properties) {
        this.properties = properties;
        this.selfId = properties.getNodeId();
        this.mode = properties.getMode();
        this.roleCache = new RoleCache();
        this.context = new GlobalContext(this);
        this.nodeState = new NodeState(context);
        this.listener = new NioListener(context);
    }

    public synchronized void start() {
        if (!start) {
            if (mode == RunMode.SINGLE) {
                startInSingleMode();
            } else {
                startInClusterMode();
            }
        }
        start = true;
    }

    private void startInSingleMode() {
        int term = nodeState.getCurrentTerm() + 1;
        this.role = Leader.builder().term(term).build();
        int index = context.pendingLog(Log.NO_OP_TYPE, new byte[0]);
        if (context.getLogContext().advanceCommitIndex(index, term)) {
            context.advanceLastApplied(index);
        }
        this.listener.listen();
    }

    private void startInClusterMode() {
        context.setConnector(new NioConnector(context));
        context.setScheduler(new SingleThreadScheduler(properties));
        context.setCluster(new Cluster(context));
        this.role = Follower.builder()
            .scheduledFuture(context.electionTimeoutTask())
            .term(nodeState.getCurrentTerm())
            .voteTo(nodeState.getVoteTo())
            .preVoteCounts(1)
            .build();
        this.listener.listen();
    }

    public synchronized void stop() {
        if (!stop) {
            nodeState.close();
            listener.stop();
            context.close();
        }
    }

    public int getTerm() {
        return role.getTerm();
    }

    public boolean isLeader() {
        return role instanceof Leader;
    }

    public boolean isFollower() {
        return role instanceof Follower;
    }

    public boolean isCandidate() {
        return role instanceof Candidate;
    }

    private int getVoteCounts(int oldVoteCounts, int newVoteCounts) {
        switch (context.getCluster().getPhase()) {
            case NEW:
                return newVoteCounts;
            case OLD_NEW:
                return oldVoteCounts | (newVoteCounts << 16);
            default:
                return oldVoteCounts;
        }
    }

    public void changeToFollower(int term, String leaderId, String voteTo, int oldVoteCounts, int newVoteCounts,
        long lastHeartBeat) {
        int voteCounts = getVoteCounts(oldVoteCounts, newVoteCounts);
        Follower follower;
        if (role.getType() != RoleType.FOLLOWER) {
            nodeState.setCurrentTerm(term);
            nodeState.setVoteTo(voteTo);
            roleCache.recycle(role);
            follower = roleCache.getFollower();
        } else {
            if (term != role.getTerm()) {
                nodeState.setCurrentTerm(term);
                nodeState.setVoteTo(voteTo);
            }
            follower = (Follower) role;
            role.cancelTask();
        }
        follower.setTerm(term);
        follower.setScheduledFuture(context.electionTimeoutTask());
        follower.setLeaderId(leaderId);
        follower.setPreVoteCounts(voteCounts);
        follower.setVoteTo(voteTo);
        follower.setLastHeartBeat(lastHeartBeat);
        this.role = follower;
    }

    public void changeToCandidate(int term, int oldVoteCounts, int newVoteCounts) {
        int voteCounts = getVoteCounts(oldVoteCounts, newVoteCounts);
        Candidate candidate;
        if (role.getType() != RoleType.CANDIDATE) {
            nodeState.setCurrentTerm(term);
            nodeState.setVoteTo(null);
            roleCache.recycle(role);
            candidate = roleCache.getCandidate();

        } else {
            if (term != role.getTerm()) {
                nodeState.setCurrentTerm(term);
                nodeState.setVoteTo(null);
            }
            candidate = (Candidate) role;
            candidate.cancelTask();
        }
        candidate.setTerm(term);
        candidate.setScheduledFuture(context.electionTimeoutTask());
        candidate.setVoteCounts(voteCounts);
        this.role = candidate;
    }

    public void changeToLeader(int term) {
        Leader leader;
        if (role.getType() != RoleType.LEADER) {
            nodeState.setCurrentTerm(term);
            nodeState.setVoteTo(null);
            roleCache.recycle(role);
            leader = roleCache.getLeader();
        } else {
            if (term != role.getTerm()) {
                nodeState.setCurrentTerm(term);
                nodeState.setVoteTo(null);
            }
            leader = (Leader) role;
            leader.cancelTask();
        }
        leader.setTerm(term);
        leader.setScheduledFuture(context.logReplicationTask());
        this.role = leader;
        int index = context.pendingLog(Log.NO_OP_TYPE, new byte[0]);
        context.getCluster().resetReplicationStates(context.getLogContext().getLastIncludeIndex() + 1, index);
        if (log.isInfoEnabled()) {
            log.info("become leader.");
            log.info("reset all node replication state with nextIndex[{}]", index);
            log.info("pending first no op log in this term, then start log replicating");
        }
    }

}
