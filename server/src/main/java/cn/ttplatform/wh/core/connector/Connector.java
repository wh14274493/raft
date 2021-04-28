package cn.ttplatform.wh.core.connector;

import cn.ttplatform.wh.core.group.EndpointMetaData;
import cn.ttplatform.wh.support.Message;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;

/**
 * @author Wang Hao
 * @date 2020/6/30 下午9:25
 */
public interface Connector {

    /**
     * create a connection with remote address
     *
     * @param endpointMetaData remote address
     * @return a socket channel
     */
    Channel connect(EndpointMetaData endpointMetaData);

    /**
     * send a message to remote
     *
     * @param message          rpc message
     * @param endpointMetaData remote server
     * @return a ChannelFuture
     */
    ChannelFuture send(Message message, EndpointMetaData endpointMetaData);

    /**
     * send a message to remote, if there is not a opened channel, do nothing
     *
     * @param message rpc message
     * @param nodeId  remote server id
     * @return a ChannelFuture
     */
    ChannelFuture send(Message message, String nodeId);

}
