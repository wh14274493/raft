package cn.ttplatform.wh.support;

import cn.ttplatform.wh.GlobalContext;
import cn.ttplatform.wh.Node;
import cn.ttplatform.wh.cmd.Command;
import cn.ttplatform.wh.cmd.RedirectCommand;
import cn.ttplatform.wh.config.ServerProperties;
import cn.ttplatform.wh.role.Follower;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author : wang hao
 * @date :  2020/8/16 0:19
 **/
@Slf4j
@Sharable
public class ServerDuplexChannelHandler extends AbstractDuplexChannelHandler {

    private final Map<Channel, LazyFlushStrategy> channelFlushStrategyMap = new ConcurrentHashMap<>();

    public ServerDuplexChannelHandler(GlobalContext context) {
        super(context);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        Channel channel = ctx.channel();
        ServerProperties properties = context.getProperties();
        channelFlushStrategyMap.put(channel, new LazyFlushStrategy(channel, properties.getLazyFlushInterval(), properties.getLazyFlushThreshold()));
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        Channel channel = ctx.channel();
        if (msg instanceof Command) {
            Command command = (Command) msg;
            String commandId = command.getId();
            if (!canHandler(command, ctx)) {
                return;
            }
            channelPool.cacheChannel(commandId, channel);
            distributor.distribute(command);
        } else {
            log.error("unknown message type, msg is {}", msg);
            channel.close();
        }
    }

    private boolean canHandler(Command command, ChannelHandlerContext ctx) {
        Node node = context.getNode();
        if (!node.isLeader()) {
            String leaderId = null;
            if (node.isFollower()) {
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
    public void flush(ChannelHandlerContext ctx) throws Exception {
        if (channelFlushStrategyMap.get(ctx.channel()).flush()) {
            super.flush(ctx);
        }
    }

    @Override
    public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
        channelFlushStrategyMap.remove(ctx.channel());
        super.close(ctx, promise);
    }

}