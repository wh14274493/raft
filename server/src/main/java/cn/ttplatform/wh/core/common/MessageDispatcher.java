package cn.ttplatform.wh.core.common;

import cn.ttplatform.wh.core.Manager;
import cn.ttplatform.wh.core.MessageHandler;
import cn.ttplatform.wh.domain.message.Message;
import cn.ttplatform.wh.exception.UnknownTypeException;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Wang Hao
 * @date 2021/2/17 2:16
 */
@Slf4j
public class MessageDispatcher implements Manager<MessageHandler> {

    private final List<MessageHandler> messageHandlers = new ArrayList<>();

    @Override
    public void register(int type, MessageHandler messageHandler) {
        if (messageHandlers.get(type) != null) {
            log.warn("wrong message type[{}], the factory for type[{}] is existed", type, type);
        }
        messageHandlers.add(type, messageHandler);
    }

    public void dispatch(Message message) {
        MessageHandler messageHandler = messageHandlers.get(message.getType());
        if (messageHandler == null) {
            throw new UnknownTypeException("unknown message type[" + message.getType() + "]");
        }
        messageHandler.handle(message);
    }
}
