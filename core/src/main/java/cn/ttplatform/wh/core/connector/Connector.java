package cn.ttplatform.wh.core.connector;

import cn.ttplatform.wh.core.MemberInfo;
import cn.ttplatform.wh.core.connector.message.Message;
import io.netty.channel.Channel;

/**
 * @author Wang Hao
 * @date 2020/6/30 下午9:25
 */
public interface Connector {

    /**
     * create a connection with remote address
     *
     * @param memberInfo remote address
     * @return a socket channel
     */
    Channel connect(MemberInfo memberInfo);

    /**
     * send a message to remote
     *
     * @param message    rpc message
     * @param memberInfo remote server
     */
    void send(Message message, MemberInfo memberInfo);

}
