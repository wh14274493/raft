package cn.ttplatform.wh.core.connector.message.handler;

import cn.ttplatform.wh.core.Node;
import cn.ttplatform.wh.core.support.AbstractMessageHandler;
import cn.ttplatform.wh.core.role.Follower;
import cn.ttplatform.wh.core.role.Role;
import cn.ttplatform.wh.core.connector.message.Message;
import cn.ttplatform.wh.core.connector.message.PreVoteResultMessage;

/**
 * @author Wang Hao
 * @date 2021/2/17 1:43
 */
public class PreVoteResultMessageHandler extends AbstractMessageHandler {

    public PreVoteResultMessageHandler(Node node) {
        super(node);
    }

    @Override
    public void doHandle(Message e) {
        PreVoteResultMessage message = (PreVoteResultMessage) e;
        boolean voted = message.isVoted();
        if (!voted || !node.isFollower()) {
            return;
        }
        Role role = node.getRole();
        int preVoteCounts = ((Follower) role).getPreVoteCounts() + 1;
        int countOfActive = node.getContext().getCluster().countOfActive();
        int currentTerm = role.getTerm();
        if (preVoteCounts > countOfActive / 2) {
            node.startElection(currentTerm + 1);
        } else {
            Follower follower = Follower.builder().preVoteCounts(preVoteCounts)
                .scheduledFuture(node.electionTimeoutTask())
                .term(currentTerm)
                .build();
            node.changeToRole(follower);
        }
    }
}
