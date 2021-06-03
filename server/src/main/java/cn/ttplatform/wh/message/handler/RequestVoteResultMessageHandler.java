package cn.ttplatform.wh.message.handler;

import cn.ttplatform.wh.message.RequestVoteResultMessage;
import cn.ttplatform.wh.constant.DistributableType;
import cn.ttplatform.wh.Node;
import cn.ttplatform.wh.GlobalContext;
import cn.ttplatform.wh.group.Cluster;
import cn.ttplatform.wh.group.Phase;
import cn.ttplatform.wh.role.Candidate;
import cn.ttplatform.wh.support.AbstractDistributableHandler;
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
    public void doHandleInClusterMode(Distributable distributable) {
        RequestVoteResultMessage message = (RequestVoteResultMessage) distributable;
        int term = message.getTerm();
        Node node = context.getNode();
        int currentTerm = node.getRole().getTerm();
        boolean voted = message.isVoted();
        if (term > currentTerm) {
            node.changeToFollower(term, null, null, 0, 0, 0L);
            log.debug("term[{}] > currentTerm[{}], become follower.", term, currentTerm);
            return;
        }
        if (node.isCandidate() && voted) {
            Candidate candidate = (Candidate) node.getRole();
            if (checkVoteCounts(message, candidate)) {
                node.changeToLeader(currentTerm);
                context.doLogReplication();
            } else {
                log.debug("need more votes");
                node.changeToCandidate(term, candidate.getOldConfigVoteCounts(), candidate.getNewConfigVoteCounts());
            }
        }
    }

    /**
     * After the leader receives the ClusterChangeCommand from the client and goes down, the following situations will occur: 1.
     * It is down during the SYNCING phase. At this time, the nodes in oldConfig do not know the configuration in newConfig, and
     * the status of the new node has not been synchronized. All nodes in oldConfig may become new nodes. At this time, the voting
     * is also Only the majority of nodes in oldConfig need to agree to be elected as the leader; 2. It is down in the OLD_NEW
     * phase. At this time, some nodes in oldConfig or newConfig have entered the OLD_NEW phase. If most nodes in oldConfig have
     * entered the OLD_NEW phase, and most nodes in newConfig have also entered the OLD_NEW phase, then At this time, only nodes
     * that have entered the OLD_NEW phase can be elected as the leader. If most of the nodes in oldConfig do not enter the
     * OLD_NEW phase, only the nodes in the oldConfig that do not enter the OLD_NEW phase can be elected as the leader (because
     * these nodes only need the consent of the majority of oldConfig to be elected as the leader, but the nodes that enter the
     * OLD_NEW phase are counted. , The majority of oldConfig and newConfig need to agree to be elected leader); 3. It is down in
     * the NEW phase. At this time, most nodes in oldConfig or newConfig have entered the OLD_NEW phase. If most of the nodes have
     * entered the NEW phase at this time, then only the nodes in newConfig can be elected as the leader. If the node is in the
     * OLD_NEW phase, the nodes in oldConfig and newConfig have a chance to be elected as the leader.
     *
     * @param e         RequestVoteResultMessage
     * @param candidate current role
     * @return res
     */
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
