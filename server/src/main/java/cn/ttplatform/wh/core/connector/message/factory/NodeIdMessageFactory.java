package cn.ttplatform.wh.core.connector.message.factory;

import cn.ttplatform.wh.core.BufferPool;
import cn.ttplatform.wh.core.common.AbstractMessageFactory;
import cn.ttplatform.wh.domain.message.Message;
import cn.ttplatform.wh.domain.message.NodeIdMessage;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

/**
 * @author Wang Hao
 * @date 2021/2/18 16:15
 */
public class NodeIdMessageFactory extends AbstractMessageFactory {

    private final Schema<NodeIdMessage> schema = RuntimeSchema.getSchema(NodeIdMessage.class);

    NodeIdMessageFactory(BufferPool<LinkedBuffer> pool) {
        super(pool);
    }

    @Override
    public Message create(byte[] content) {
        NodeIdMessage message = new NodeIdMessage();
        ProtostuffIOUtil.mergeFrom(content, message, schema);
        return message;
    }

    @Override
    public byte[] getBytes(Message message, LinkedBuffer buffer) {
        return ProtostuffIOUtil.toByteArray((NodeIdMessage) message, schema, buffer);
    }
}
