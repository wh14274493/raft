package cn.ttplatform.lc.core.rpc.message.factory;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Wang Hao
 * @date 2021/2/18 16:58
 */
public class MessageFactoryManager {

    private static final List<MessageFactory> FACTORY_LIST = new ArrayList<>();

    public void register(int type, MessageFactory factory) {
        if (FACTORY_LIST.get(type) != null) {
            throw new IllegalArgumentException(
                String.format("wrong message type[%d], the factory for type[%d] is existed", type, type));
        }
        FACTORY_LIST.add(type, factory);
    }

    public MessageFactory getFactory(int type) {
        MessageFactory messageFactory = FACTORY_LIST.get(type);
        if (messageFactory == null) {
            throw new IllegalArgumentException(
                String.format("wrong message type[%d], the factory for type[%d] not found", type, type));
        }
        return messageFactory;
    }
}
