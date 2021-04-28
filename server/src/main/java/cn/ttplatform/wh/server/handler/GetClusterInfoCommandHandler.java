package cn.ttplatform.wh.server.handler;

import cn.ttplatform.wh.cmd.Command;
import cn.ttplatform.wh.cmd.GetClusterInfoResultCommand;
import cn.ttplatform.wh.core.NodeContext;
import cn.ttplatform.wh.core.group.Cluster;
import cn.ttplatform.wh.core.support.AbstractMessageHandler;
import cn.ttplatform.wh.core.support.ChannelPool;
import cn.ttplatform.wh.support.Message;

/**
 * @author Wang Hao
 * @date 2021/4/28 10:56
 */
public class GetClusterInfoCommandHandler extends AbstractMessageHandler {

    public GetClusterInfoCommandHandler(NodeContext context) {
        super(context);
    }

    @Override
    public void doHandle(Message message) {
        Cluster cluster = context.getCluster();
        GetClusterInfoResultCommand command = GetClusterInfoResultCommand.builder()
            .leader(cluster.getSelfId())
            .mode(cluster.getMode().toString())
            .phase(cluster.getPhase().toString())
            .newConfig(cluster.getNewConfigMap().toString())
            .oldConfig(cluster.getEndpointMap().toString())
            .build();
        ChannelPool.reply(((Command) message).getId(), command);
    }
}
