package cn.ttplatform.wh.cmd.handler;

import cn.ttplatform.wh.cmd.SetCommand;
import cn.ttplatform.wh.core.Node;
import cn.ttplatform.wh.core.connector.message.Message;
import cn.ttplatform.wh.core.support.AbstractMessageHandler;

/**
 * @author Wang Hao
 * @date 2021/2/21 15:56
 */
public class SetCommandHandler extends AbstractMessageHandler {

    public SetCommandHandler(Node node) {
        super(node);
    }

    @Override
    public void doHandle(Message e) {
        node.pendingEntry((SetCommand) e);
    }

}
