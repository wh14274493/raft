package cn.ttplatform.wh.server.handler;

import cn.ttplatform.wh.cmd.Message;
import cn.ttplatform.wh.cmd.SetCommand;
import cn.ttplatform.wh.core.NodeContext;
import cn.ttplatform.wh.core.support.AbstractMessageHandler;

/**
 * @author Wang Hao
 * @date 2021/2/21 15:56
 */
public class SetCommandMessageHandler extends AbstractMessageHandler {

    public SetCommandMessageHandler(NodeContext context) {
        super(context);
    }

    @Override
    public void doHandle(Message e) {
        context.pendingEntry((SetCommand) e);
    }

}
