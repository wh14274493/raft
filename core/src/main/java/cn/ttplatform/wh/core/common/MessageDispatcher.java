package cn.ttplatform.wh.core.common;

import cn.ttplatform.wh.domain.message.Message;
import cn.ttplatform.wh.exception.UnknownTypeException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Wang Hao
 * @date 2021/2/17 2:16
 */
@Slf4j
public class MessageDispatcher implements Manager<MessageHandler> {

    private final Map<Integer, MessageHandler> handlerMap;

    public MessageDispatcher() {
        this.handlerMap = new HashMap<>();
    }

    @Override
    public void register(int type, MessageHandler messageHandler) {
        if (handlerMap.get(type) != null) {
            log.warn("wrong message type[{}], the factory for type[{}] is existed", type, type);
        }
        handlerMap.put(type, messageHandler);
    }

    public void dispatch(Message message) {
        MessageHandler messageHandler = handlerMap.get(message.getType());
        if (messageHandler == null) {
            throw new UnknownTypeException("unknown message type[" + message.getType() + "]");
        }
        messageHandler.handle(message);
    }
}
