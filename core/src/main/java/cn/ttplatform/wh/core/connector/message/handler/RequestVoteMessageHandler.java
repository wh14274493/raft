package cn.ttplatform.wh.core.connector.message.handler;

import cn.ttplatform.wh.core.ClusterMember;
import cn.ttplatform.wh.core.Node;
import cn.ttplatform.wh.core.NodeContext;
import cn.ttplatform.wh.core.support.AbstractMessageHandler;
import cn.ttplatform.wh.core.role.Follower;
import cn.ttplatform.wh.core.role.Role;
import cn.ttplatform.wh.core.connector.message.Message;
import cn.ttplatform.wh.core.connector.message.RequestVoteMessage;
import cn.ttplatform.wh.core.connector.message.RequestVoteResultMessage;

/**
 * @author Wang Hao
 * @date 2021/2/17 1:46
 */
public class RequestVoteMessageHandler extends AbstractMessageHandler {

    public RequestVoteMessageHandler(Node node) {
        super(node);
    }

    @Override
    public void doHandle(Message e) {
        RequestVoteMessage message = (RequestVoteMessage) e;
        String candidateId = message.getCandidateId();
        NodeContext context = node.getContext();
        ClusterMember member = context.getCluster().find(candidateId);
        int term = message.getTerm();
        Role role = node.getRole();
        int currentTerm = role.getTerm();
        int lastLogIndex = message.getLastLogIndex();
        int lastLogTerm = message.getLastLogTerm();
        RequestVoteResultMessage requestVoteResultMessage = RequestVoteResultMessage.builder()
            .isVoted(Boolean.FALSE).term(currentTerm)
            .build();
        if (term < currentTerm) {
            node.sendMessage(requestVoteResultMessage, member);
            return;
        }
        if (term > currentTerm) {
            boolean voted = !context.getLog().isNewerThan(lastLogIndex, lastLogTerm);
            requestVoteResultMessage.setTerm(term);
            if (voted) {
                requestVoteResultMessage.setVoted(Boolean.TRUE);
                Follower follower = Follower.builder()
                    .scheduledFuture(node.electionTimeoutTask())
                    .voteTo(candidateId)
                    .term(term)
                    .build();
                node.changeToRole(follower);
            }
            node.sendMessage(requestVoteResultMessage, member);
            return;
        }
        if (node.isFollower()) {
            boolean voted = !context.getLog().isNewerThan(lastLogIndex, lastLogTerm);
            if (isEmpty(((Follower) role).getVoteTo()) && voted) {
                requestVoteResultMessage.setVoted(Boolean.TRUE);
                Follower follower = Follower.builder()
                    .scheduledFuture(node.electionTimeoutTask())
                    .voteTo(candidateId)
                    .term(term)
                    .build();
                node.changeToRole(follower);
            }
        }
        node.sendMessage(requestVoteResultMessage, member);
    }

    private boolean isEmpty(String s) {
        return s == null || "".equals(s);
    }
}
