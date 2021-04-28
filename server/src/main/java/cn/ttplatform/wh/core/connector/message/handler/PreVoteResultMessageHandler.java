package cn.ttplatform.wh.core.connector.message.handler;

import cn.ttplatform.wh.constant.DistributableType;
import cn.ttplatform.wh.core.NodeContext;
import cn.ttplatform.wh.core.connector.message.PreVoteResultMessage;
import cn.ttplatform.wh.core.group.Cluster;
import cn.ttplatform.wh.core.group.Phase;
import cn.ttplatform.wh.core.role.Follower;
import cn.ttplatform.wh.core.support.AbstractDistributableHandler;
import cn.ttplatform.wh.support.Distributable;
import cn.ttplatform.wh.support.Message;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Wang Hao
 * @date 2021/2/17 1:43
 */
@Slf4j
public class PreVoteResultMessageHandler extends AbstractDistributableHandler {

    public PreVoteResultMessageHandler(NodeContext context) {
        super(context);
    }

    @Override
    public int getHandlerType() {
        return DistributableType.PRE_VOTE_RESULT;
    }

    @Override
    public void doHandle(Distributable distributable) {
        PreVoteResultMessage message = (PreVoteResultMessage) distributable;
        boolean voted = message.isVoted();
        if (!voted || !context.isFollower()) {
            return;
        }
        Follower role = (Follower) context.getNode().getRole();
        int currentTerm = role.getTerm();
        if (checkVoteCounts(message, role)) {
            log.debug("startElection");
            context.startElection(currentTerm + 1);
        } else {
            log.debug("need more votes.");
            context.changeToFollower(currentTerm, null, null, role.getPreVoteCounts());
        }
    }

    private boolean checkVoteCounts(Message e, Follower role) {
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
                newCounts = role.incrementNewCountsAndGet();
                log.debug("phase is NEW, a major of newConfig is {}", newCounts);
                return newCounts > countOfNewConfig / 2;
            case OLD_NEW:
                if (cluster.inNewConfig(e.getSourceId())) {
                    newCounts = role.incrementNewCountsAndGet();
                    oldCounts = role.getOldConfigPreVoteCounts();
                    if (log.isDebugEnabled()) {
                        log.debug("phase is OLD_NEW.");
                        log.debug("receive a pre vote msg from newConfigNode[{}].", e.getSourceId());
                    }
                } else {
                    newCounts = role.getNewConfigPreVoteCounts();
                    oldCounts = role.incrementOldCountsAndGet();
                    if (log.isDebugEnabled()) {
                        log.debug("phase is OLD_NEW.");
                        log.debug("receive a pre vote msg from oldConfigNode[{}].", e.getSourceId());
                    }
                }
                if (log.isDebugEnabled()) {
                    log.debug("newCounts is {}.", newCounts);
                    log.debug("oldCounts is {}.", oldCounts);
                }
                return newCounts > countOfNewConfig / 2 && oldCounts > countOfOldConfig / 2;
            default:
                oldCounts = role.incrementOldCountsAndGet();
                if (log.isDebugEnabled()) {
                    log.debug("phase is {}.", phase);
                    log.debug("oldCounts is {}.", oldCounts);
                }
                return oldCounts > countOfOldConfig / 2;
        }
    }
}
