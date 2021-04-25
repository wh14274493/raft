package cn.ttplatform.wh.server.handler;

import cn.ttplatform.wh.cmd.ClusterChangeCommand;
import cn.ttplatform.wh.common.Message;
import cn.ttplatform.wh.cmd.RequestFailedCommand;
import cn.ttplatform.wh.constant.ErrorMessage;
import cn.ttplatform.wh.core.Cluster;
import cn.ttplatform.wh.core.Cluster.Phase;
import cn.ttplatform.wh.core.NodeContext;
import cn.ttplatform.wh.core.support.AbstractMessageHandler;
import cn.ttplatform.wh.core.support.ChannelCache;
import io.netty.channel.Channel;

/**
 * @author Wang Hao
 * @date 2021/4/23 23:26
 */
public class ClusterChangeCommandHandler extends AbstractMessageHandler {

    private final RequestFailedCommand requestFailedCommand = new RequestFailedCommand();

    public ClusterChangeCommandHandler(NodeContext context) {
        super(context);
    }

    @Override
    public void doHandle(Message e) {
        ClusterChangeCommand cmd = (ClusterChangeCommand) e;
        Cluster cluster = context.getCluster();
        if (cluster.getPhase() != Phase.STABLE) {
            Channel channel = ChannelCache.getChannel(cmd.getId());
            if (channel != null) {
                requestFailedCommand.setFailedMessage(ErrorMessage.CLUSTER_CHANGE_IN_PROGRESS);
                channel.writeAndFlush(requestFailedCommand);
            }
        } else {
            cluster.enterSyncingPhase(cmd);
        }
    }
}
