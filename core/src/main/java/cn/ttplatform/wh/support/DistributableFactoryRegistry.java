package cn.ttplatform.wh.support;

import cn.ttplatform.wh.exception.UnknownTypeException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Wang Hao
 * @date 2021/4/29 0:45
 */
public class DistributableFactoryRegistry implements Registry<DistributableFactory> {

    private static final int COUNT_OF_FACTORY = 18;
    private final Map<Integer, DistributableFactory> factoryMap;

    public DistributableFactoryRegistry() {
        this.factoryMap = new HashMap<>((int) (COUNT_OF_FACTORY / 0.75f + 1));
    }

    @Override
    public void register(DistributableFactory factory) {
        factoryMap.put(factory.getFactoryType(), factory);
    }

    public DistributableFactory getFactory(int type) {
        DistributableFactory factory = factoryMap.get(type);
        if (factory == null) {
            throw new UnknownTypeException("unknown message type[" + type + "]");
        }
        return factory;
    }
}
