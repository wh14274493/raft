package cn.ttplatform.wh.core.support;

import cn.ttplatform.wh.core.connector.message.Message;

/**
 * @author Wang Hao
 * @date 2021/2/18 11:26
 */
public interface MessageFactory {

    /**
     * use the byte array to create an {@link Message} Object
     *
     * @param content a serialized byte array
     * @return a deserialized object
     */
    Message create(byte[] content);

    /**
     * use protostuff to serialize a message object
     *
     * @param message {@link Message} obj
     * @return a serialized byte array
     */
    byte[] getBytes(Message message);
}
