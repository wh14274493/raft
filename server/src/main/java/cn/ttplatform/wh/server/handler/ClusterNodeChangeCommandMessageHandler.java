package cn.ttplatform.wh.server.handler;

import cn.ttplatform.wh.cmd.ClusterNodeChangeCommand;
import cn.ttplatform.wh.cmd.Message;
import cn.ttplatform.wh.core.NodeContext;
import cn.ttplatform.wh.core.support.AbstractMessageHandler;

/**
 * @author Wang Hao
 * @date 2021/4/23 23:26
 */
public class ClusterNodeChangeCommandMessageHandler extends AbstractMessageHandler {

    public ClusterNodeChangeCommandMessageHandler(NodeContext context) {
        super(context);
    }

    @Override
    public void doHandle(Message e) {
        ClusterNodeChangeCommand cmd = (ClusterNodeChangeCommand) e;
    }
}
