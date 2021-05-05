package cn.ttplatform.wh.core.support;

import cn.ttplatform.wh.config.RunMode;
import cn.ttplatform.wh.constant.ErrorMessage;
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
        RunMode mode = context.getNode().getMode();
        if (mode == RunMode.SINGLE) {
            context.getExecutor().execute(() -> doHandleInSingleMode(distributable));
        } else {
            context.getExecutor().execute(() -> doHandleInClusterMode(distributable));
        }
    }

    /**
     * process a distributable msg in cluster mode
     *
     * @param distributable distributable msg
     */
    public abstract void doHandleInClusterMode(Distributable distributable);

    /**
     * process a distributable msg in single mode
     *
     * @param distributable distributable msg
     */
    public void doHandleInSingleMode(Distributable distributable) {
        throw new UnsupportedOperationException(ErrorMessage.MESSAGE_TYPE_ERROR);
    }
}
