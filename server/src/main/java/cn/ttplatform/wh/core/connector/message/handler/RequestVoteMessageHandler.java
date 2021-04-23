package cn.ttplatform.wh.core.connector.message.handler;

import cn.ttplatform.wh.cmd.Message;
import cn.ttplatform.wh.core.ClusterMember;
import cn.ttplatform.wh.core.NodeContext;
import cn.ttplatform.wh.core.connector.message.RequestVoteMessage;
import cn.ttplatform.wh.core.connector.message.RequestVoteResultMessage;
import cn.ttplatform.wh.core.role.Follower;
import cn.ttplatform.wh.core.role.Role;
import cn.ttplatform.wh.core.support.AbstractMessageHandler;

/**
 * @author Wang Hao
 * @date 2021/2/17 1:46
 */
public class RequestVoteMessageHandler extends AbstractMessageHandler {

    public RequestVoteMessageHandler(NodeContext context) {
        super(context);
    }

    @Override
    public void doHandle(Message e) {
        RequestVoteMessage message = (RequestVoteMessage) e;
        ClusterMember member = context.getCluster().find(message.getCandidateId());
        context.sendMessage(process(message), member);
    }

    private RequestVoteResultMessage process(RequestVoteMessage message) {
        int term = message.getTerm();
        Role role = context.getNode().getRole();
        int currentTerm = role.getTerm();
        int lastLogIndex = message.getLastLogIndex();
        int lastLogTerm = message.getLastLogTerm();
        String candidateId = message.getCandidateId();
        RequestVoteResultMessage requestVoteResultMessage = RequestVoteResultMessage.builder()
            .isVoted(Boolean.FALSE).term(currentTerm)
            .build();
        if (term < currentTerm) {
            return requestVoteResultMessage;
        }
        if (term > currentTerm) {
            boolean voted = !context.getLog().isNewerThan(lastLogIndex, lastLogTerm);
            requestVoteResultMessage.setTerm(term);
            if (voted) {
                requestVoteResultMessage.setVoted(Boolean.TRUE);
                context.changeToFollower(term, null, candidateId, 0);
            }
            return requestVoteResultMessage;
        }
        if (context.isFollower()) {
            boolean voted = !context.getLog().isNewerThan(lastLogIndex, lastLogTerm);
            String voteTo = ((Follower) role).getVoteTo();
            if (voted && (voteTo == null || "".equals(voteTo))) {
                requestVoteResultMessage.setVoted(Boolean.TRUE);
                context.changeToFollower(term, null, candidateId, 0);
            }
        }
        return requestVoteResultMessage;
    }

}
