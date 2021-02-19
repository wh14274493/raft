package cn.ttplatform.lc.core.rpc.message.handler;

import cn.ttplatform.lc.core.Node;
import cn.ttplatform.lc.core.role.Follower;
import cn.ttplatform.lc.core.role.Role;
import cn.ttplatform.lc.core.rpc.message.domain.Message;
import cn.ttplatform.lc.core.rpc.message.domain.PreVoteResultMessage;

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
        int countOfActive = node.getContext().cluster().countOfActive();
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
