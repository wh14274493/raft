package cn.ttplatform.wh.core.connector.message.handler;

import cn.ttplatform.wh.core.Node;
import cn.ttplatform.wh.core.log.entry.LogEntry;
import cn.ttplatform.wh.support.Message;
import cn.ttplatform.wh.core.NodeContext;
import cn.ttplatform.wh.core.connector.message.RequestVoteResultMessage;
import cn.ttplatform.wh.core.group.Cluster;
import cn.ttplatform.wh.core.group.Phase;
import cn.ttplatform.wh.core.role.Candidate;
import cn.ttplatform.wh.core.role.Leader;
import cn.ttplatform.wh.core.support.AbstractMessageHandler;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Wang Hao
 * @date 2021/2/17 1:48
 */
@Slf4j
public class RequestVoteResultMessageHandler extends AbstractMessageHandler {

    public RequestVoteResultMessageHandler(NodeContext context) {
        super(context);
    }

    @Override
    public void doHandle(Message e) {
        RequestVoteResultMessage message = (RequestVoteResultMessage) e;
        int term = message.getTerm();
        Node node = context.getNode();
        int currentTerm = node.getRole().getTerm();
        boolean voted = message.isVoted();
        if (term > currentTerm) {
            context.changeToFollower(term, null, null, 0);
            return;
        }
        if (context.isCandidate() && voted) {
            Candidate candidate = (Candidate) node.getRole();
            if (checkVoteCounts(message, candidate)) {
                Leader leader = Leader.builder().term(currentTerm).scheduledFuture(context.logReplicationTask())
                    .build();
                context.changeToRole(leader);
                int nextIndex = context.getLog().getNextIndex();
                context.getCluster().resetReplicationStates(nextIndex);
                context.pendingLog(LogEntry.NO_OP_TYPE, new byte[0]);
                if (log.isDebugEnabled()) {
                    log.debug("become leader.");
                    log.debug("reset all node replication state by nextIndex[{}]", nextIndex);
                    log.debug("pending first no op log in this term, then start log replicating");
                }
                context.doLogReplication();
            } else {
                log.debug("need more votes");
                context.changeToCandidate(term, candidate.getVoteCounts());
            }
        }
    }

    private boolean checkVoteCounts(Message e, Candidate candidate) {
        Cluster cluster = context.getCluster();
        int countOfOldConfig = cluster.countOfOldConfig();
        int countOfNewConfig = cluster.countOfNewConfig();
        if (log.isDebugEnabled()) {
            log.debug("countOfOldConfig is {}", countOfOldConfig);
            log.debug("countOfNewConfig is {}", countOfNewConfig);
        }
        Phase phase = cluster.getPhase();
        int oldCounts;
        int newCounts;
        switch (phase) {
            case NEW:
                newCounts = candidate.incrementNewCountsAndGet();
                log.debug("phase is NEW, and the newCounts is {}.", newCounts);
                return newCounts > countOfNewConfig / 2;
            case OLD_NEW:
                log.debug("phase is OLD_NEW.");
                if (cluster.inNewConfig(e.getSourceId())) {
                    log.debug("receive a vote msg from newConfigNode[{}].", e.getSourceId());
                    newCounts = candidate.incrementNewCountsAndGet();
                    oldCounts = candidate.getOldConfigVoteCounts();
                } else {
                    log.debug("receive a vote msg from oldConfigNode[{}].", e.getSourceId());
                    newCounts = candidate.getNewConfigVoteCounts();
                    oldCounts = candidate.incrementOldCountsAndGet();
                }
                if (log.isDebugEnabled()) {
                    log.debug("newCounts is {}.", newCounts);
                    log.debug("oldCounts is {}.", oldCounts);
                }
                return newCounts > countOfNewConfig / 2 && oldCounts > countOfOldConfig / 2;
            default:
                oldCounts = candidate.incrementOldCountsAndGet();
                if (log.isDebugEnabled()) {
                    log.debug("phase is {}.", phase);
                    log.debug("oldCounts is {}.", oldCounts);
                }
                return oldCounts > countOfOldConfig / 2;
        }
    }
}
