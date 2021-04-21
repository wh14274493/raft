package cn.ttplatform.wh.core.handler;

import cn.ttplatform.wh.cmd.SetResponseCommand;
import cn.ttplatform.wh.core.ClientContext;
import cn.ttplatform.wh.core.connector.message.Message;
import cn.ttplatform.wh.core.support.Future;
import cn.ttplatform.wh.core.support.MessageHandler;
import cn.ttplatform.wh.core.support.RequestRecord;

/**
 * @author Wang Hao
 * @date 2021/4/20 13:49
 */
public class SetResponseCommandHandler implements MessageHandler {

    private final ClientContext context;

    public SetResponseCommandHandler(ClientContext context) {
        this.context = context;
    }

    @Override
    public void handle(Message message) {
        SetResponseCommand cmd = (SetResponseCommand) message;
        @SuppressWarnings("unchecked")
        RequestRecord<SetResponseCommand> requestRecord = context.removeRequestRecord(cmd.getId());
        Future<SetResponseCommand> future = requestRecord.getFuture();
        future.put(cmd);
    }
}
