package cn.ttplatform.wh.support;

import cn.ttplatform.wh.cmd.Command;
import cn.ttplatform.wh.cmd.RedirectCommand;
import cn.ttplatform.wh.GlobalContext;
import cn.ttplatform.wh.Node;
import cn.ttplatform.wh.role.Follower;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

/**
 * @author : wang hao
 * @date :  2020/8/16 0:19
 **/
@Slf4j
@Sharable
public class CoreDuplexChannelHandler extends ChannelDuplexHandler {

    private final GlobalContext context;
    private final CommonDistributor distributor;
    private final ChannelPool channelPool;

    public CoreDuplexChannelHandler(GlobalContext context) {
        this.context = context;
        this.distributor = context.getDistributor();
        this.channelPool = context.getChannelPool();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
//        Channel channel = ctx.channel();
//        channel.eventLoop().scheduleAtFixedRate(channel::flush, 100, 100, TimeUnit.MILLISECONDS);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        Channel channel = ctx.channel();
        if (msg instanceof Command) {
            Command command = (Command) msg;
            String commandId = command.getId();
//            log.debug("receive a command {} from {}.", command, commandId);
            if (!canHandler(command, ctx)) {
                return;
            }
            channelPool.cacheChannel(commandId, channel);
//            recordIds(commandId, channel);
            distributor.distribute(command);
        } else if (msg instanceof Message) {
            String sourceId = ((Message) msg).getSourceId();
            log.debug("receive a msg {} from {}.", msg, sourceId);
            channelPool.cacheChannel(sourceId, channel);
//            recordIds(sourceId, channel);
            distributor.distribute((Message) msg);
        } else {
            log.error("unknown message type, msg is {}", msg);
            channel.close();
        }
    }

    private void recordIds(String id, Channel channel) {
        AttributeKey<Set<String>> attributeKey = AttributeKey.valueOf("ids");
        Attribute<Set<String>> idsAttribute = channel.attr(attributeKey);
        Set<String> ids = idsAttribute.get();
        if (ids == null) {
            ids = new HashSet<>();
            idsAttribute.set(ids);
        }
        ids.add(id);
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
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error(cause.toString());
        Channel channel = ctx.channel();
        if (!channel.isOpen()) {
            AttributeKey<Set<String>> idsAttributeKey = AttributeKey.valueOf("ids");
            Attribute<Set<String>> attribute = channel.attr(idsAttributeKey);
            Optional.ofNullable(attribute.get()).orElse(Collections.emptySet()).forEach(channelPool::removeChannel);
        }
    }

    @Override
    public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
        super.close(ctx, promise);
        log.debug("close the channel[{}].", ctx.channel());
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            ctx.channel().close();
            log.info("fire IdleStateEvent[{}].", evt);
        } else {
            log.info("fire Event[{}].", evt);
            super.userEventTriggered(ctx, evt);
        }
    }
}