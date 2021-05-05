package cn.ttplatform.wh.core.listener.handler;

import cn.ttplatform.wh.cmd.GetCommand;
import cn.ttplatform.wh.constant.DistributableType;
import cn.ttplatform.wh.core.GlobalContext;
import cn.ttplatform.wh.core.support.AbstractDistributableHandler;
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
        context.getStateMachine().replyGetResult((GetCommand) distributable);
    }

    @Override
    public void doHandleInClusterMode(Distributable distributable) {
        int nextIndex = context.getLog().getNextIndex();
        int key = nextIndex - 1;
        context.getStateMachine().addGetTasks(key, (GetCommand) distributable);
    }

    @Override
    public int getHandlerType() {
        return DistributableType.GET_COMMAND;
    }
}
