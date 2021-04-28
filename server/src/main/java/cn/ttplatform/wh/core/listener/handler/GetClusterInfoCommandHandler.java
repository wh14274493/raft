package cn.ttplatform.wh.core.listener.handler;

import cn.ttplatform.wh.cmd.Command;
import cn.ttplatform.wh.cmd.GetClusterInfoResultCommand;
import cn.ttplatform.wh.constant.DistributableType;
import cn.ttplatform.wh.core.NodeContext;
import cn.ttplatform.wh.core.group.Cluster;
import cn.ttplatform.wh.core.support.AbstractDistributableHandler;
import cn.ttplatform.wh.core.support.ChannelPool;
import cn.ttplatform.wh.support.Distributable;

/**
 * @author Wang Hao
 * @date 2021/4/28 10:56
 */
public class GetClusterInfoCommandHandler extends AbstractDistributableHandler {

    public GetClusterInfoCommandHandler(NodeContext context) {
        super(context);
    }

    @Override
    public void doHandle(Distributable distributable) {
        Cluster cluster = context.getCluster();
        GetClusterInfoResultCommand respCommand = GetClusterInfoResultCommand.builder()
            .leader(cluster.getSelfId())
            .mode(cluster.getMode().toString())
            .phase(cluster.getPhase().toString())
            .newConfig(cluster.getNewConfigMap().toString())
            .oldConfig(cluster.getEndpointMap().toString())
            .build();
        ChannelPool.reply(((Command)distributable).getId(), respCommand);
    }

    @Override
    public int getHandlerType() {
        return DistributableType.GET_CLUSTER_INFO_COMMAND;
    }
}
