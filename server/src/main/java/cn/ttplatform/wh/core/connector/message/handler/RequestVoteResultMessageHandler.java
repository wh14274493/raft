package cn.ttplatform.wh.core.connector.message.handler;

import cn.ttplatform.wh.common.Message;
import cn.ttplatform.wh.core.NodeContext;
import cn.ttplatform.wh.core.connector.message.RequestVoteResultMessage;
import cn.ttplatform.wh.core.role.Candidate;
import cn.ttplatform.wh.core.role.Leader;
import cn.ttplatform.wh.core.role.Role;
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
        Role role = context.getNode().getRole();
        int currentTerm = role.getTerm();
        boolean voted = message.isVoted();
        if (term > currentTerm) {
            context.changeToFollower(term, null, null, 0);
            return;
        }
        if (context.isCandidate() && voted) {
            int voteCounts = ((Candidate) role).getVoteCounts() + 1;
            int countOfActive = context.getCluster().countOfCluster();
            if (voteCounts > countOfActive / 2) {
                Leader leader = Leader.builder().term(currentTerm).scheduledFuture(context.logReplicationTask())
                    .build();
                context.changeToRole(leader);
                log.info("voteCounts[{}] > countOfActive/2[{}], become leader.", voteCounts, countOfActive / 2);
                int nextIndex = context.getLog().getNextIndex();
                context.getCluster().resetReplicationStates(nextIndex);
                log.info("reset all node replication state by nextIndex[{}]", nextIndex);
                context.doLogReplication();
            } else {
                context.changeToCandidate(term, voteCounts);
            }
        }
    }
}
