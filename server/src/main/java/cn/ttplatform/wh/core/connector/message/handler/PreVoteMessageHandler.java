package cn.ttplatform.wh.core.connector.message.handler;

import cn.ttplatform.wh.core.NodeContext;
import cn.ttplatform.wh.core.connector.message.PreVoteMessage;
import cn.ttplatform.wh.core.connector.message.PreVoteResultMessage;
import cn.ttplatform.wh.core.support.AbstractMessageHandler;
import cn.ttplatform.wh.support.Message;

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
        PreVoteResultMessage preVoteResultMessage = PreVoteResultMessage.builder()
            .isVoted(!context.getLog().isNewerThan(message.getLastLogIndex(), message.getLastLogTerm()))
            .build();
        context.sendMessage(preVoteResultMessage, message.getSourceId());
    }
}
