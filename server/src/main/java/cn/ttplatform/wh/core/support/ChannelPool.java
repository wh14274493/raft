package cn.ttplatform.wh.core.support;

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

    private ChannelPool() {
    }

    private static final Map<String, Channel> CACHE = new ConcurrentHashMap<>();

    public static void cacheChannel(String key, Channel channel) {
        CACHE.computeIfAbsent(key, s -> {
            log.debug("key:{} exist in cache.", key);
            channel.closeFuture().addListener(future -> {
                if (future.isSuccess()) {
                    CACHE.remove(s);
                }
            });
            return channel;
        });
    }

    public static Channel removeChannel(String key) {
        return CACHE.remove(key);
    }

    public static Channel getChannel(String key) {
        return CACHE.get(key);
    }

    public static ChannelFuture reply(String id, Command command) {
        Channel channel = removeChannel(id);
        if (channel != null) {
            return channel.writeAndFlush(command).addListener(future -> {
                if (future.isSuccess()) {
                    log.debug("reply {} success", id);
                } else {
                    log.debug("reply {} failed", id);
                }
            });
        }
        log.debug("channel for {} is null", id);
        return null;
    }

}
