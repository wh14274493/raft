package cn.ttplatform.wh.core.listener.handler;

import cn.ttplatform.wh.cmd.Command;
import cn.ttplatform.wh.cmd.GetClusterInfoResultCommand;
import cn.ttplatform.wh.config.RunMode;
import cn.ttplatform.wh.constant.DistributableType;
import cn.ttplatform.wh.core.GlobalContext;
import cn.ttplatform.wh.core.group.Cluster;
import cn.ttplatform.wh.core.support.AbstractDistributableHandler;
import cn.ttplatform.wh.core.support.ChannelPool;
import cn.ttplatform.wh.support.Distributable;

/**
 * @author Wang Hao
 * @date 2021/4/28 10:56
 */
public class GetClusterInfoCommandHandler extends AbstractDistributableHandler {

    public GetClusterInfoCommandHandler(GlobalContext context) {
        super(context);
    }

    @Override
    public void doHandleInSingleMode(Distributable distributable) {
        String requestId = ((Command) distributable).getId();
        GetClusterInfoResultCommand respCommand = GetClusterInfoResultCommand.builder()
            .id(requestId)
            .leader(context.getNode().getSelfId())
            .mode(RunMode.SINGLE.toString())
            .build();
        ChannelPool.reply(requestId, respCommand);
    }

    @Override
    public void doHandleInClusterMode(Distributable distributable) {
        Cluster cluster = context.getCluster();
        String requestId = ((Command) distributable).getId();
        GetClusterInfoResultCommand respCommand = GetClusterInfoResultCommand.builder()
            .id(requestId)
            .leader(cluster.getSelfId())
            .mode(context.getNode().getMode().toString())
            .phase(cluster.getPhase().toString())
            .newConfig(cluster.getNewConfigMap().toString())
            .oldConfig(cluster.getEndpointMap().toString())
            .build();
        ChannelPool.reply(requestId, respCommand);
    }

    @Override
    public int getHandlerType() {
        return DistributableType.GET_CLUSTER_INFO_COMMAND;
    }
}
