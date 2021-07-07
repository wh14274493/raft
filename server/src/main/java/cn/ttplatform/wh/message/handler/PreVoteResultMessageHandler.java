package cn.ttplatform.wh.message.handler;

import cn.ttplatform.wh.message.PreVoteResultMessage;
import cn.ttplatform.wh.constant.DistributableType;
import cn.ttplatform.wh.GlobalContext;
import cn.ttplatform.wh.Node;
import cn.ttplatform.wh.group.Cluster;
import cn.ttplatform.wh.group.Phase;
import cn.ttplatform.wh.role.Follower;
import cn.ttplatform.wh.support.AbstractDistributableHandler;
import cn.ttplatform.wh.support.Distributable;
import cn.ttplatform.wh.support.Message;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Wang Hao
 * @date 2021/2/17 1:43
 */
@Slf4j
public class PreVoteResultMessageHandler extends AbstractDistributableHandler {

    public PreVoteResultMessageHandler(GlobalContext context) {
        super(context);
    }

    @Override
    public int getHandlerType() {
        return DistributableType.PRE_VOTE_RESULT;
    }

    @Override
    public void doHandleInClusterMode(Distributable distributable) {
        PreVoteResultMessage message = (PreVoteResultMessage) distributable;
        Node node = context.getNode();
        boolean voted = message.isVoted();
        if (!voted || !node.isFollower()) {
            return;
        }
        Follower role = (Follower) context.getNode().getRole();
        int currentTerm = role.getTerm();
        if (checkVoteCounts(message, role)) {
            context.startElection(currentTerm + 1);
        } else {
            log.debug("need more votes.");
            node.changeToFollower(currentTerm,
                null, role.getVoteTo(),
                role.getOldConfigPreVoteCounts(),
                role.getNewConfigPreVoteCounts(),
                0L);
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
        int oldCounts = role.getOldConfigPreVoteCounts();
        int newCounts = role.getNewConfigPreVoteCounts();
        switch (phase) {
            case NEW:
                newCounts = role.incrementNewCountsAndGet();
                log.debug("phase is NEW, a major of newConfig is {}", newCounts);
                return newCounts > countOfNewConfig / 2;
            case OLD_NEW:
                if (cluster.inNewConfig(e.getSourceId())) {
                    newCounts = role.incrementNewCountsAndGet();
                    if (log.isDebugEnabled()) {
                        log.debug("phase is OLD_NEW.");
                        log.debug("receive a pre vote msg from newConfigNode[{}].", e.getSourceId());
                    }
                }
                if (cluster.inOldConfig(e.getSourceId())) {
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
