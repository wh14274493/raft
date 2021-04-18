package cn.ttplatform.wh.core.common;

import cn.ttplatform.wh.exception.UnknownTypeException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Wang Hao
 * @date 2021/2/18 16:58
 */
@Slf4j
public class MessageFactoryManager implements Manager<MessageFactory> {

    private final Map<Integer, MessageFactory> factoryMap = new HashMap<>();

    @Override
    public void register(int type, MessageFactory factory) {
        if (factoryMap.get(type) != null) {
            log.warn("wrong message type[{}], the factory for type[{}] is existed", type, type);
        }
        factoryMap.put(type, factory);
    }

    public MessageFactory getFactory(int type) {
        MessageFactory messageFactory = factoryMap.get(type);
        if (messageFactory == null) {
            throw new UnknownTypeException("unknown message type[" + type + "]");
        }
        return messageFactory;
    }
}
