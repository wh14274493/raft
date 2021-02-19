package cn.ttplatform.lc.core.rpc.message.handler;

import cn.ttplatform.lc.core.rpc.message.domain.Message;

/**
 * @author Wang Hao
 * @date 2021/2/17 0:27
 */
public interface MessageHandler {

    void handle(Message message);

}
