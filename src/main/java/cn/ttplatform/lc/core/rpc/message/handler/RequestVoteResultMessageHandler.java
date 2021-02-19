package cn.ttplatform.lc.core.rpc.message.handler;

import cn.ttplatform.lc.core.Node;
import cn.ttplatform.lc.core.NodeContext;
import cn.ttplatform.lc.core.role.Candidate;
import cn.ttplatform.lc.core.role.Follower;
import cn.ttplatform.lc.core.role.Leader;
import cn.ttplatform.lc.core.role.Role;
import cn.ttplatform.lc.core.rpc.message.domain.Message;
import cn.ttplatform.lc.core.rpc.message.domain.RequestVoteResultMessage;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Wang Hao
 * @date 2021/2/17 1:48
 */
@Slf4j
public class RequestVoteResultMessageHandler extends AbstractMessageHandler {

    public RequestVoteResultMessageHandler(Node node) {
        super(node);
    }

    @Override
    public void doHandle(Message e) {
        RequestVoteResultMessage message = (RequestVoteResultMessage) e;
        int term = message.getTerm();
        Role role = node.getRole();
        int currentTerm = role.getTerm();
        boolean voted = message.isVoted();
        if (term > currentTerm) {
            Follower follower = Follower.builder()
                .scheduledFuture(node.electionTimeoutTask())
                .term(term)
                .build();
            node.changeToRole(follower);
            return;
        }
        if (node.isCandidate() && voted) {
            NodeContext context = node.getContext();
            int voteCounts = ((Candidate) role).getVoteCounts() + 1;
            int countOfActive = context.cluster().countOfActive();
            if (voteCounts > countOfActive / 2) {
                Leader leader = Leader.builder().term(currentTerm).scheduledFuture(node.logReplicationTask()).build();
                node.changeToRole(leader);
                log.debug("voteCounts[{}] > countOfActive/2[{}], become leader.", voteCounts, countOfActive / 2);
                int nextIndex = context.log().getNextIndex();
                context.cluster().resetReplicationStates(nextIndex);
                log.debug("reset all node replication state by nextIndex[{}]", nextIndex);
                node.doLogReplication();
            } else {
                Candidate candidate = Candidate.builder()
                    .scheduledFuture(node.electionTimeoutTask())
                    .term(currentTerm)
                    .voteCounts(voteCounts)
                    .build();
                node.changeToRole(candidate);
            }
        }
    }
}
