package cn.ttplatform.wh.core.handler;

import cn.ttplatform.wh.cmd.GetResponseCommand;
import cn.ttplatform.wh.core.ClientContext;
import cn.ttplatform.wh.core.connector.message.Message;
import cn.ttplatform.wh.core.support.Future;
import cn.ttplatform.wh.core.support.MessageHandler;
import cn.ttplatform.wh.core.support.RequestRecord;

/**
 * @author Wang Hao
 * @date 2021/4/20 13:49
 */
public class GetResponseCommandHandler implements MessageHandler {

    private final ClientContext context;

    public GetResponseCommandHandler(ClientContext context) {
        this.context = context;
    }

    @Override
    public void handle(Message message) {
        GetResponseCommand cmd = (GetResponseCommand) message;
        @SuppressWarnings("unchecked")
        RequestRecord<GetResponseCommand> requestRecord = context.removeRequestRecord(cmd.getId());
        Future<GetResponseCommand> future = requestRecord.getFuture();
        future.put(cmd);
    }
}
