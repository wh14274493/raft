package cn.ttplatform.wh.core.support;


import cn.ttplatform.wh.core.connector.message.Message;

/**
 * @author Wang Hao
 * @date 2021/2/17 0:27
 */
public interface MessageHandler {

    void handle(Message message);
}
