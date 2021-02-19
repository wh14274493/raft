package cn.ttplatform.lc.core.rpc.message.factory;

import cn.ttplatform.lc.core.rpc.message.domain.Message;
import io.protostuff.LinkedBuffer;

/**
 * @author Wang Hao
 * @date 2021/2/18 11:26
 */
public interface MessageFactory {

    Message create(byte[] content);

    byte[] getBytes(LinkedBuffer buffer, Message message);
}
