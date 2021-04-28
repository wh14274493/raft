package cn.ttplatform.wh.server.handler;

import cn.ttplatform.wh.cmd.GetCommand;
import cn.ttplatform.wh.support.Message;
import cn.ttplatform.wh.core.NodeContext;
import cn.ttplatform.wh.core.support.AbstractMessageHandler;

/**
 * @author Wang Hao
 * @date 2021/2/21 16:01
 */
public class GetCommandHandler extends AbstractMessageHandler {

    public GetCommandHandler(NodeContext context) {
        super(context);
    }

    @Override
    public void doHandle(Message e) {
        int nextIndex = context.getLog().getNextIndex();
        int key = nextIndex - 1;
        context.getStateMachine().addGetTasks(key, (GetCommand) e);
    }

}
