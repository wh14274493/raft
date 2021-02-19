package cn.ttplatform.lc.core.rpc.nio;

import cn.ttplatform.lc.core.rpc.message.LinkedBufferPool;
import cn.ttplatform.lc.core.rpc.message.MessageContext;
import cn.ttplatform.lc.core.rpc.message.domain.Message;
import cn.ttplatform.lc.core.rpc.message.factory.MessageFactory;
import cn.ttplatform.lc.core.rpc.message.factory.MessageFactoryManager;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import io.protostuff.LinkedBuffer;
import lombok.extern.slf4j.Slf4j;

/**
 * @author : wang hao
 * @date :  2020/8/16 0:14
 **/
@Slf4j
public class ProtostuffEncoder extends MessageToByteEncoder<Message> {

    private final LinkedBufferPool bufferPool;
    private final MessageFactoryManager factoryManager;

    public ProtostuffEncoder(MessageContext context) {
        this.bufferPool = context.getPool();
        this.factoryManager = context.getFactoryManager();
    }

    /**
     * Encode a message into a {@link ByteBuf}. This method will be called for each written message that can be handled
     * by this encoder.
     *
     * @param ctx the {@link ChannelHandlerContext} which this {@link MessageToByteEncoder} belongs to
     * @param msg the message to encode
     * @param out the {@link ByteBuf} into which the encoded message will be written
     */
    @Override
    protected void encode(ChannelHandlerContext ctx, Message msg, ByteBuf out) {
        byte[] encode = encode(msg);
        out.writeInt(msg.getType());
        out.writeLong(encode.length);
        out.writeBytes(encode);
    }

    private byte[] encode(Message message) {
        LinkedBuffer buffer = bufferPool.allocate();
        try {
            MessageFactory messageFactory = factoryManager.getFactory(message.getType());
            return messageFactory.getBytes(buffer, message);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            bufferPool.recycle(buffer);
        }
    }
}
