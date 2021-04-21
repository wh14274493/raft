package cn.ttplatform.wh.core.handler;

import cn.ttplatform.wh.cmd.RequestFailedCommand;
import cn.ttplatform.wh.core.ClientContext;
import cn.ttplatform.wh.core.connector.message.Message;
import cn.ttplatform.wh.core.support.MessageHandler;
import cn.ttplatform.wh.core.support.RequestRecord;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Wang Hao
 * @date 2021/4/20 13:50
 */
@Slf4j
public class RequestFailedCommandHandler implements MessageHandler {

    private final ClientContext context;

    public RequestFailedCommandHandler(ClientContext context) {
        this.context = context;
    }

    @Override
    public void handle(Message message) {
        RequestFailedCommand cmd = (RequestFailedCommand) message;
        RequestRecord<?> requestRecord = context.removeRequestRecord(cmd.getId());
        log.error("Request failed, failed type is {}", cmd.getFailedType());
        requestRecord.getFuture().interruptAllWaiters();
    }
}
