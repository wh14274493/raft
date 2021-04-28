package cn.ttplatform.wh.support;

import cn.ttplatform.wh.exception.UnknownTypeException;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Wang Hao
 * @date 2021/2/18 16:58
 */
@Slf4j
public class MessageFactoryManager implements Manager<Factory> {

    private final Map<Integer, Factory> factoryMap = new HashMap<>();

    @Override
    public void register(int type, Factory factory) {
        if (factoryMap.get(type) != null) {
            log.warn("wrong message type[{}], the factory for type[{}] is existed", type, type);
        }
        factoryMap.put(type, factory);
    }

    public Factory getFactory(int type) {
        Factory factory = factoryMap.get(type);
        if (factory == null) {
            throw new UnknownTypeException("unknown message type[" + type + "]");
        }
        return factory;
    }
}
