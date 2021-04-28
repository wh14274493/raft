package cn.ttplatform.wh.core.listener.handler;

import cn.ttplatform.wh.cmd.GetCommand;
import cn.ttplatform.wh.constant.DistributableType;
import cn.ttplatform.wh.core.NodeContext;
import cn.ttplatform.wh.core.support.AbstractDistributableHandler;
import cn.ttplatform.wh.support.Distributable;

/**
 * @author Wang Hao
 * @date 2021/2/21 16:01
 */
public class GetCommandHandler extends AbstractDistributableHandler {

    public GetCommandHandler(NodeContext context) {
        super(context);
    }

    @Override
    public void doHandle(Distributable distributable) {
        int nextIndex = context.getLog().getNextIndex();
        int key = nextIndex - 1;
        context.getStateMachine().addGetTasks(key, (GetCommand) distributable);
    }

    @Override
    public int getHandlerType() {
        return DistributableType.GET_COMMAND;
    }
}
