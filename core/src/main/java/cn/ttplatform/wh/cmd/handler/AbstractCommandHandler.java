package cn.ttplatform.wh.cmd.handler;

import cn.ttplatform.wh.cmd.Command;
import cn.ttplatform.wh.cmd.RedirectCommand;
import cn.ttplatform.wh.cmd.RequestFailedCommand;
import cn.ttplatform.wh.core.Node;
import cn.ttplatform.wh.core.connector.message.Message;
import cn.ttplatform.wh.core.role.Follower;
import cn.ttplatform.wh.core.support.AbstractMessageHandler;
import cn.ttplatform.wh.core.support.ChannelCache;
import io.netty.channel.Channel;

/**
 * @author Wang Hao
 * @date 2021/4/20 9:28
 */
public abstract class AbstractCommandHandler extends AbstractMessageHandler {

    protected AbstractCommandHandler(Node node) {
        super(node);
    }

    @Override
    public void handle(Message message) {
        Command cmd = (Command) message;
        if (!node.isLeader()) {
            Message resp;
            if (node.isFollower()) {
                Follower follower = (Follower) node.getRole();
                String leaderId = follower.getLeaderId();
                if (leaderId == null || "".equals(leaderId)) {
                    resp = RequestFailedCommand.builder().id(cmd.getId())
                        .failedType(RequestFailedCommand.CLUSTER_IS_UNSTABLE)
                        .build();
                } else {
                    resp = RedirectCommand.builder().id(cmd.getId())
                        .leaderId(leaderId)
                        .members(node.getContext().getCluster().listAllMemberInfo())
                        .build();
                }
            } else {
                resp = RequestFailedCommand.builder().id(cmd.getId())
                    .failedType(RequestFailedCommand.CLUSTER_IS_UNSTABLE)
                    .build();
            }
            Channel channel = ChannelCache.getChannel(cmd.getId());
            channel.writeAndFlush(resp);
            return;
        }
        super.handle(message);
    }
}
