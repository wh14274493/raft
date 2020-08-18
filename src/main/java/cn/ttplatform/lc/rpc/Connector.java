package cn.ttplatform.lc.rpc;

import cn.ttplatform.lc.node.ClusterMember;
import cn.ttplatform.lc.rpc.message.Message;
import cn.ttplatform.lc.rpc.nio.NioChannel;
import java.nio.channels.Channel;

/**
 * @author Wang Hao
 * @date 2020/6/30 下午9:25
 */
public interface Connector {

    /**
     * @param member create a connection with remote address
     * @return a socket channel
     */
    NioChannel connect(ClusterMember member);

    /**
     * send a message to remote
     *
     * @param message rpc message
     * @param channel remote address
     */
    void write(Message message, NioChannel channel);

    /**
     * free the resources
     */
    void close();
}
