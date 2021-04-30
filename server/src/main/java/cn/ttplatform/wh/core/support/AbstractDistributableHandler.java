package cn.ttplatform.wh.core.support;

import cn.ttplatform.wh.core.GlobalContext;
import cn.ttplatform.wh.support.Distributable;
import cn.ttplatform.wh.support.DistributableHandler;

/**
 * @author Wang Hao
 * @date 2021/4/29 0:03
 */
public abstract class AbstractDistributableHandler implements DistributableHandler {

    protected GlobalContext context;

    protected AbstractDistributableHandler(GlobalContext context) {
        this.context = context;
    }

    @Override
    public void handle(Distributable distributable) {
        context.getExecutor().execute(() -> doHandle(distributable));
    }

    /**
     * process a distributable msg
     * @param distributable distributable msg
     */
    public abstract void doHandle(Distributable distributable);
}