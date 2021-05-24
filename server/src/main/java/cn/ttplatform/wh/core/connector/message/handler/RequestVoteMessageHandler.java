package cn.ttplatform.wh.core.connector.message.handler;

import cn.ttplatform.wh.constant.DistributableType;
import cn.ttplatform.wh.core.GlobalContext;
import cn.ttplatform.wh.core.Node;
import cn.ttplatform.wh.core.connector.message.RequestVoteMessage;
import cn.ttplatform.wh.core.connector.message.RequestVoteResultMessage;
import cn.ttplatform.wh.core.role.Follower;
import cn.ttplatform.wh.core.role.Role;
import cn.ttplatform.wh.core.support.AbstractDistributableHandler;
import cn.ttplatform.wh.support.Distributable;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Wang Hao
 * @date 2021/2/17 1:46
 */
@Slf4j
public class RequestVoteMessageHandler extends AbstractDistributableHandler {

    public RequestVoteMessageHandler(GlobalContext context) {
        super(context);
    }

    @Override
    public int getHandlerType() {
        return DistributableType.REQUEST_VOTE;
    }

    @Override
    public void doHandleInClusterMode(Distributable distributable) {
        RequestVoteMessage message = (RequestVoteMessage) distributable;
        context.sendMessage(process(message), message.getSourceId());
    }

    private RequestVoteResultMessage process(RequestVoteMessage message) {
        Node node = context.getNode();
        Role role = node.getRole();
        if (node.isFollower() && System.currentTimeMillis() - ((Follower) role).getLastHeartBeat() < context.getProperties()
            .getMinElectionTimeout()) {
            log.debug("current leader is alive, reject this request vote message.");
            return null;
        }
        int term = message.getTerm();
        int currentTerm = role.getTerm();
        int lastLogIndex = message.getLastLogIndex();
        int lastLogTerm = message.getLastLogTerm();
        String candidateId = message.getCandidateId();
        RequestVoteResultMessage requestVoteResultMessage = RequestVoteResultMessage.builder()
            .isVoted(Boolean.FALSE).term(currentTerm)
            .build();
        if (term < currentTerm) {
            log.debug("the term[{}] < currentTerm[{}], reject this request vote message.", term, currentTerm);
            return requestVoteResultMessage;
        }
        if (term > currentTerm) {
            boolean voted = !context.getLogContext().isNewerThan(lastLogIndex, lastLogTerm);
            requestVoteResultMessage.setTerm(term);
            requestVoteResultMessage.setVoted(voted);
            log.debug("the term[{}] > currentTerm[{}], and the vote result is {}.", term, currentTerm, voted);
            if (voted) {
                log.debug("become follower and vote to {}", candidateId);
                node.changeToFollower(term, null, candidateId, 0, 0, 0L);
            }
            return requestVoteResultMessage;
        }
        if (node.isFollower()) {
            boolean voted = !context.getLogContext().isNewerThan(lastLogIndex, lastLogTerm);
            log.debug("the term[{}] == currentTerm[{}], and the vote result is {}.", term, currentTerm, voted);
            String voteTo = ((Follower) role).getVoteTo();
            if (voted && (voteTo == null || "".equals(voteTo))) {
                requestVoteResultMessage.setVoted(voted);
                log.debug("at this point, having not voted for any other node, so become follower and vote to {}", candidateId);
                node.changeToFollower(term, null, candidateId, 0, 0, 0L);
            }
        }
        return requestVoteResultMessage;
    }

}
