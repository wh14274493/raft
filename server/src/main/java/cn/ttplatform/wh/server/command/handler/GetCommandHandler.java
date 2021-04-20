package cn.ttplatform.wh.server.command.handler;

import cn.ttplatform.wh.core.Node;
import cn.ttplatform.wh.core.support.AbstractMessageHandler;
import cn.ttplatform.wh.cmd.GetCommand;
import cn.ttplatform.wh.core.connector.message.Message;

/**
 * @author Wang Hao
 * @date 2021/2/21 16:01
 */
public class GetCommandHandler extends AbstractMessageHandler {

    public GetCommandHandler(Node node) {
        super(node);
    }

    @Override
    public void doHandle(Message e) {
        node.handleGetCommand((GetCommand) e);
    }

}
