package cn.ttplatform.wh.core.connector.message.handler;

import cn.ttplatform.wh.common.Message;
import cn.ttplatform.wh.core.NodeContext;
import cn.ttplatform.wh.core.connector.message.PreVoteResultMessage;
import cn.ttplatform.wh.core.role.Follower;
import cn.ttplatform.wh.core.role.Role;
import cn.ttplatform.wh.core.support.AbstractMessageHandler;

/**
 * @author Wang Hao
 * @date 2021/2/17 1:43
 */
public class PreVoteResultMessageHandler extends AbstractMessageHandler {

    public PreVoteResultMessageHandler(NodeContext context) {
        super(context);
    }

    @Override
    public void doHandle(Message e) {
        PreVoteResultMessage message = (PreVoteResultMessage) e;
        boolean voted = message.isVoted();
        if (!voted || !context.isFollower()) {
            return;
        }
        Role role = context.getNode().getRole();
        int preVoteCounts = ((Follower) role).getPreVoteCounts() + 1;
        int countOfCluster = context.getCluster().countOfCluster();
        int currentTerm = role.getTerm();
        if (preVoteCounts > countOfCluster / 2) {
            context.startElection(currentTerm + 1);
        } else {
            context.changeToFollower(currentTerm, null, null, preVoteCounts);
        }
    }
}
