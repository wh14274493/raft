package cn.ttplatform.wh.core.support;

import io.netty.channel.Channel;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Wang Hao
 * @date 2021/2/18 23:15
 */
public class ChannelCache {

    private ChannelCache() {
    }

    private static final Map<String, Channel> CACHE = new ConcurrentHashMap<>();


    public static void cacheChannel(String key, Channel channel) {
        CACHE.put(key, channel);
    }

    public static Channel removeChannel(String key) {
        return CACHE.remove(key);
    }

    public static Channel getChannel(String key) {
        return CACHE.get(key);
    }

}
