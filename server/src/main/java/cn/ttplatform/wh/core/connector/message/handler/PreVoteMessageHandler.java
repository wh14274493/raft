package cn.ttplatform.wh.core.connector.message.handler;

import cn.ttplatform.wh.common.Message;
import cn.ttplatform.wh.core.Endpoint;
import cn.ttplatform.wh.core.NodeContext;
import cn.ttplatform.wh.core.connector.message.PreVoteMessage;
import cn.ttplatform.wh.core.connector.message.PreVoteResultMessage;
import cn.ttplatform.wh.core.support.AbstractMessageHandler;

/**
 * @author Wang Hao
 * @date 2021/2/17 1:39
 */
public class PreVoteMessageHandler extends AbstractMessageHandler {

    public PreVoteMessageHandler(NodeContext context) {
        super(context);
    }

    @Override
    public void doHandle(Message e) {
        PreVoteMessage message = (PreVoteMessage) e;
        Endpoint endpoint = context.getCluster().find(message.getNodeId());
        int lastLogIndex = message.getLastLogIndex();
        int lastLogTerm = message.getLastLogTerm();
        PreVoteResultMessage preVoteResultMessage = PreVoteResultMessage.builder()
            .isVoted(Boolean.TRUE)
            .build();
        if (context.getLog().isNewerThan(lastLogIndex, lastLogTerm)) {
            preVoteResultMessage.setVoted(Boolean.FALSE);
        }
        context.sendMessage(preVoteResultMessage, endpoint);
    }
}
