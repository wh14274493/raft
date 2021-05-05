package cn.ttplatform.wh.core.connector.message.handler;

import cn.ttplatform.wh.constant.DistributableType;
import cn.ttplatform.wh.core.GlobalContext;
import cn.ttplatform.wh.core.Node;
import cn.ttplatform.wh.core.connector.message.PreVoteMessage;
import cn.ttplatform.wh.core.connector.message.PreVoteResultMessage;
import cn.ttplatform.wh.core.role.Follower;
import cn.ttplatform.wh.core.role.Role;
import cn.ttplatform.wh.core.support.AbstractDistributableHandler;
import cn.ttplatform.wh.support.Distributable;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Wang Hao
 * @date 2021/2/17 1:39
 */
@Slf4j
public class PreVoteMessageHandler extends AbstractDistributableHandler {

    public PreVoteMessageHandler(GlobalContext context) {
        super(context);
    }

    @Override
    public int getHandlerType() {
        return DistributableType.PRE_VOTE;
    }

    @Override
    public void doHandleInClusterMode(Distributable distributable) {
        Node node = context.getNode();
        Role role = node.getRole();
        if (node.isFollower() && System.currentTimeMillis() - ((Follower) role).getLastHeartBeat() < context.getProperties()
            .getMinElectionTimeout()) {
            log.debug("current leader is alive, reject this pre request vote message.");
            return;
        }
        PreVoteMessage message = (PreVoteMessage) distributable;
        PreVoteResultMessage preVoteResultMessage = PreVoteResultMessage.builder()
            .isVoted(!context.getLog().isNewerThan(message.getLastLogIndex(), message.getLastLogTerm()))
            .build();
        context.sendMessage(preVoteResultMessage, message.getSourceId());
    }
}
