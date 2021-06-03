package cn.ttplatform.wh.message.handler;

import cn.ttplatform.wh.constant.DistributableType;
import cn.ttplatform.wh.constant.ErrorMessage;
import cn.ttplatform.wh.GlobalContext;
import cn.ttplatform.wh.message.SyncingMessage;
import cn.ttplatform.wh.support.AbstractDistributableHandler;
import cn.ttplatform.wh.support.Distributable;

/**
 * @author Wang Hao
 * @date 2021/5/3 10:27
 */
public class SyncingMessageHandler extends AbstractDistributableHandler {

    public SyncingMessageHandler(GlobalContext context) {
        super(context);
    }

    @Override
    public int getHandlerType() {
        return DistributableType.SYNCING;
    }

    @Override
    public void doHandleInClusterMode(Distributable distributable) {
        throw new UnsupportedOperationException(ErrorMessage.MESSAGE_TYPE_ERROR);
    }

    @Override
    public void doHandleInSingleMode(Distributable distributable) {
        SyncingMessage syncingMessage = (SyncingMessage) distributable;
        context.enterClusterMode();
        context.getNode().changeToFollower(syncingMessage.getTerm(), syncingMessage.getSourceId(), null, 0, 0, 0);
    }
}
