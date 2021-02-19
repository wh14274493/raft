package cn.ttplatform.lc.core.rpc.message.handler;

import cn.ttplatform.lc.core.ClusterMember;
import cn.ttplatform.lc.core.Node;
import cn.ttplatform.lc.core.NodeContext;
import cn.ttplatform.lc.core.role.Follower;
import cn.ttplatform.lc.core.rpc.message.domain.Message;
import cn.ttplatform.lc.core.rpc.message.domain.PreVoteMessage;
import cn.ttplatform.lc.core.rpc.message.domain.PreVoteResultMessage;

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
