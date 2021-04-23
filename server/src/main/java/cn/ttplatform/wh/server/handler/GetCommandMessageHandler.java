package cn.ttplatform.wh.server.handler;

import cn.ttplatform.wh.cmd.GetCommand;
import cn.ttplatform.wh.cmd.Message;
import cn.ttplatform.wh.core.NodeContext;
import cn.ttplatform.wh.core.support.AbstractMessageHandler;

/**
 * @author Wang Hao
 * @date 2021/2/21 16:01
 */
public class GetCommandMessageHandler extends AbstractMessageHandler {

    public GetCommandMessageHandler(NodeContext context) {
        super(context);
    }

    @Override
    public void doHandle(Message e) {
        context.handleGetCommand((GetCommand) e);
    }

}
