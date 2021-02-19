package cn.ttplatform.lc.core.rpc.message.handler;

import cn.ttplatform.lc.exception.UnknownTypeException;
import cn.ttplatform.lc.core.rpc.message.domain.Message;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @author Wang Hao
 * @date 2021/2/17 2:16
 */
public class MessageDispatcher {

    private final List<MessageHandler> messageHandlers = new CopyOnWriteArrayList<>();

    public void register(int type, MessageHandler messageHandler) {
        messageHandlers.add(type, messageHandler);
    }

    public void handle(Message message) {
        MessageHandler messageHandler = messageHandlers.get(message.getType());
        if (messageHandler == null) {
            throw new UnknownTypeException("unknown message type[" + message.getClass() + "]");
        }
        messageHandler.handle(message);
    }
}
