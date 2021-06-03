package cn.ttplatform.wh.support;

import cn.ttplatform.wh.exception.UnknownTypeException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Wang Hao
 * @date 2021/4/28 23:58
 */
public class CommonDistributor implements Registry<DistributableHandler>, Distributor<Distributable> {

    private static final int COUNT_OF_HANDLER = 12;
    private final Map<Integer, DistributableHandler> handlerMap;

    public CommonDistributor() {
        this.handlerMap = new HashMap<>((int) (COUNT_OF_HANDLER / 0.75f + 1));
    }

    @Override
    public void distribute(Distributable distributable) {
        DistributableHandler handler = handlerMap.get(distributable.getType());
        if (handler == null) {
            throw new UnknownTypeException("unknown message type[" + distributable.getType() + "]");
        }
        handler.handle(distributable);
    }

    @Override
    public void register(DistributableHandler handler) {
        handlerMap.put(handler.getHandlerType(),handler);
    }

}
