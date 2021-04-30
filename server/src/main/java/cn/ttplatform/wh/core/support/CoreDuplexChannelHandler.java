package cn.ttplatform.wh.core.support;

import cn.ttplatform.wh.cmd.Command;
import cn.ttplatform.wh.cmd.RedirectCommand;
import cn.ttplatform.wh.core.GlobalContext;
import cn.ttplatform.wh.core.role.Follower;
import cn.ttplatform.wh.support.Message;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

/**
 * @author : wang hao
 * @date :  2020/8/16 0:19
 **/
@Slf4j
public class CoreDuplexChannelHandler extends ChannelDuplexHandler {

    private final GlobalContext context;
    private final CommonDistributor distributor;

    public CoreDuplexChannelHandler(GlobalContext context) {
        this.context = context;
        this.distributor = context.getDistributor();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof Command) {
            Command command = (Command) msg;
            log.debug("receive a command {} from {}.", command, command.getId());
            if (!canHandler(command, ctx)) {
                return;
            }
            ChannelPool.cacheChannel(command.getId(), ctx.channel());
            distributor.distribute(command);
        } else if (msg instanceof Message) {
            String remoteId = ((Message) msg).getSourceId();
            log.debug("receive a msg {} from {}.", msg, remoteId);
            ChannelPool.cacheChannel(remoteId, ctx.channel());
            distributor.distribute((Message) msg);
        } else {
            log.error("unknown message type, msg is {}", msg);
        }
    }

    private boolean canHandler(Command command, ChannelHandlerContext ctx) {
        if (!context.isLeader()) {
            String leaderId = null;
            if (context.isFollower()) {
                Follower role = (Follower) context.getNode().getRole();
                leaderId = role.getLeaderId();
            }
            log.info("current role is not a leader, redirect request to node[id={}]", leaderId);
            ctx.channel().writeAndFlush(RedirectCommand.builder().id(command.getId()).leader(leaderId)
                .endpointMetaData(context.getCluster().getAllEndpointMetaData().toString()).build());
            return false;
        }
        return true;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error(cause.toString());
    }
}
