package cn.ttplatform.wh.handler;

import cn.ttplatform.wh.cmd.GetCommand;
import cn.ttplatform.wh.constant.DistributableType;
import cn.ttplatform.wh.GlobalContext;
import cn.ttplatform.wh.support.AbstractDistributableHandler;
import cn.ttplatform.wh.support.Distributable;

/**
 * @author Wang Hao
 * @date 2021/2/21 16:01
 */
public class GetCommandHandler extends AbstractDistributableHandler {

    public GetCommandHandler(GlobalContext context) {
        super(context);
    }

    @Override
    public void doHandleInSingleMode(Distributable distributable) {
        context.replyGetResult((GetCommand) distributable);
    }

    @Override
    public void doHandleInClusterMode(Distributable distributable) {
        int nextIndex = context.getDataManager().getNextIndex();
        int key = nextIndex - 1;
        context.addGetTasks(key, (GetCommand) distributable);
    }

    @Override
    public int getHandlerType() {
        return DistributableType.GET_COMMAND;
    }
}
