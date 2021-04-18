package cn.ttplatform.wh.core.connector;

import cn.ttplatform.wh.core.ClusterMember;
import cn.ttplatform.wh.domain.message.Message;
import io.netty.channel.Channel;

/**
 * @author Wang Hao
 * @date 2020/6/30 下午9:25
 */
public interface Connector {

    /**
     * @param member create a connection with remote address
     * @return a socket channel
     */
    Channel connect(ClusterMember member);

    /**
     * send a message to remote
     *
     * @param message rpc message
     * @param member remote server
     */
    void send(Message message, ClusterMember member);

    /**
     * free the resources
     */
    void close();
}
