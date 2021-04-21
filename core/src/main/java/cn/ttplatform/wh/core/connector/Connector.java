package cn.ttplatform.wh.core.connector;

import cn.ttplatform.wh.core.MemberInfo;
import cn.ttplatform.wh.core.connector.message.Message;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import java.net.InetSocketAddress;

/**
 * @author Wang Hao
 * @date 2020/6/30 下午9:25
 */
public interface Connector {

    /**
     * create a connection with remote address
     *
     * @param address remote address
     * @return a socket channel
     */
    Channel connect(InetSocketAddress address);

    /**
     * send a message to remote
     *
     * @param message rpc message
     * @param info    remote server info
     */
    ChannelFuture send(Message message, MemberInfo info);

}
