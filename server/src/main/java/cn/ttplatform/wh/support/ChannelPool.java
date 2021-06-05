package cn.ttplatform.wh.support;

import cn.ttplatform.wh.cmd.Command;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Wang Hao
 * @date 2021/2/18 23:15
 */
@Slf4j
public class ChannelPool {

    private final Map<String, Channel> channelMap = new ConcurrentHashMap<>();

    public void cacheChannel(String key, Channel channel) {
//        CACHE.computeIfAbsent(key, s -> {
//            log.debug("key:{} is not exist in cache.", key);
//            channel.closeFuture().addListener(future -> {
//                if (future.isSuccess()) {
//                    CACHE.remove(s);
//                }
//            });
//            return channel;
//        });
        channelMap.put(key, channel);
    }

    public Channel removeChannel(String key) {
        return channelMap.remove(key);
    }

    public Channel getChannel(String key) {
        return channelMap.get(key);
    }

    public ChannelFuture reply(String id, Command command) {
        Channel channel = removeChannel(id);
        if (channel != null) {
            return channel.writeAndFlush(command);
        }
        log.debug("channel for {} is null", id);
        return null;
    }

}
