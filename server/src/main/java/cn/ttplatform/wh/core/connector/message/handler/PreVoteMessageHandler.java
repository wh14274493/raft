package cn.ttplatform.wh.core.connector.message.handler;

import cn.ttplatform.wh.constant.DistributableType;
import cn.ttplatform.wh.core.NodeContext;
import cn.ttplatform.wh.core.connector.message.PreVoteMessage;
import cn.ttplatform.wh.core.connector.message.PreVoteResultMessage;
import cn.ttplatform.wh.core.support.AbstractDistributableHandler;
import cn.ttplatform.wh.support.Distributable;

/**
 * @author Wang Hao
 * @date 2021/2/17 1:39
 */
public class PreVoteMessageHandler extends AbstractDistributableHandler {

    public PreVoteMessageHandler(NodeContext context) {
        super(context);
    }

    @Override
    public int getHandlerType() {
        return DistributableType.PRE_VOTE;
    }

    @Override
    public void doHandle(Distributable distributable) {
        PreVoteMessage message = (PreVoteMessage) distributable;
        PreVoteResultMessage preVoteResultMessage = PreVoteResultMessage.builder()
            .isVoted(!context.getLog().isNewerThan(message.getLastLogIndex(), message.getLastLogTerm()))
            .build();
        context.sendMessage(preVoteResultMessage, message.getSourceId());
    }
}
