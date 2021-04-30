package cn.ttplatform.wh.core.connector.message.handler;

import cn.ttplatform.wh.constant.DistributableType;
import cn.ttplatform.wh.core.Node;
import cn.ttplatform.wh.core.GlobalContext;
import cn.ttplatform.wh.core.connector.message.RequestVoteResultMessage;
import cn.ttplatform.wh.core.group.Cluster;
import cn.ttplatform.wh.core.group.Phase;
import cn.ttplatform.wh.core.role.Candidate;
import cn.ttplatform.wh.core.support.AbstractDistributableHandler;
import cn.ttplatform.wh.support.Distributable;
import cn.ttplatform.wh.support.Message;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Wang Hao
 * @date 2021/2/17 1:48
 */
@Slf4j
public class RequestVoteResultMessageHandler extends AbstractDistributableHandler {

    public RequestVoteResultMessageHandler(GlobalContext context) {
        super(context);
    }

    @Override
    public int getHandlerType() {
        return DistributableType.REQUEST_VOTE_RESULT;
    }

    @Override
    public void doHandle(Distributable distributable) {
        RequestVoteResultMessage message = (RequestVoteResultMessage) distributable;
        int term = message.getTerm();
        Node node = context.getNode();
        int currentTerm = node.getRole().getTerm();
        boolean voted = message.isVoted();
        if (term > currentTerm) {
            context.changeToFollower(term, null, null, 0, 0, 0L);
            log.debug("term[{}] > currentTerm[{}], become follower.", term, currentTerm);
            return;
        }
        if (context.isCandidate() && voted) {
            Candidate candidate = (Candidate) node.getRole();
            if (checkVoteCounts(message, candidate)) {
                context.changeToLeader(currentTerm);
                context.doLogReplication();
            } else {
                log.debug("need more votes");
                context.changeToCandidate(term, candidate.getOldConfigVoteCounts(), candidate.getNewConfigVoteCounts());
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
        int oldCounts = candidate.getOldConfigVoteCounts();
        int newCounts = candidate.getNewConfigVoteCounts();
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
                }
                if (cluster.inOldConfig(e.getSourceId())) {
                    log.debug("receive a vote msg from oldConfigNode[{}].", e.getSourceId());
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
