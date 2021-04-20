package cn.ttplatform.wh.core.connector.message.handler;

import cn.ttplatform.wh.core.ClusterMember;
import cn.ttplatform.wh.core.Node;
import cn.ttplatform.wh.core.NodeContext;
import cn.ttplatform.wh.core.support.AbstractMessageHandler;
import cn.ttplatform.wh.core.role.Follower;
import cn.ttplatform.wh.core.connector.message.Message;
import cn.ttplatform.wh.core.connector.message.PreVoteMessage;
import cn.ttplatform.wh.core.connector.message.PreVoteResultMessage;

/**
 * @author Wang Hao
 * @date 2021/2/17 1:39
 */
public class PreVoteMessageHandler extends AbstractMessageHandler {

    public PreVoteMessageHandler(Node node) {
        super(node);
    }

    @Override
    public void doHandle(Message e) {
        NodeContext context = node.getContext();
        if (!node.isFollower() || System.currentTimeMillis() - ((Follower) node.getRole()).getLastHeartBeat() < context
            .config()
            .getMinElectionTimeout()) {
            return;
        }
        PreVoteMessage message = (PreVoteMessage) e;
        ClusterMember clusterMember = context.cluster().find(message.getNodeId());
        int lastLogIndex = message.getLastLogIndex();
        int lastLogTerm = message.getLastLogTerm();
        PreVoteResultMessage preVoteResultMessage = PreVoteResultMessage.builder()
            .isVoted(Boolean.TRUE)
            .build();
        if (context.log().isNewerThan(lastLogIndex, lastLogTerm)) {
            preVoteResultMessage.setVoted(Boolean.FALSE);
        }
        node.sendMessage(preVoteResultMessage, clusterMember);
    }
}
