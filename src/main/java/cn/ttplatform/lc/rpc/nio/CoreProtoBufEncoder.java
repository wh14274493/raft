package cn.ttplatform.lc.rpc.nio;

import cn.ttplatform.lc.constant.RpcMessageType;
import cn.ttplatform.lc.rpc.message.Message;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;
import lombok.extern.slf4j.Slf4j;

/**
 * @author : wang hao
 * @description : CoreProtoBufCodec
 * @date :  2020/8/16 0:14
 **/
@Slf4j
public class CoreProtoBufEncoder extends MessageToByteEncoder<Message> {

    /**
     * Encode a message into a {@link ByteBuf}. This method will be called for each written message that can be handled
     * by this encoder.
     *
     * @param ctx the {@link ChannelHandlerContext} which this {@link MessageToByteEncoder} belongs to
     * @param msg the message to encode
     * @param out the {@link ByteBuf} into which the encoded message will be written
     * @throws Exception is thrown if an error occurs
     */
    @Override
    protected void encode(ChannelHandlerContext ctx, Message msg, ByteBuf out) throws Exception {
        byte[] encode = encode(msg);
        out.writeInt(msg.getType());
        out.writeLong(encode.length);
        out.writeBytes(encode);
    }

    private byte[] encode(Message message){
        Schema schema = RuntimeSchema.getSchema(message.getClass());
        LinkedBuffer linkedBuffer = LinkedBuffer.allocate(LinkedBuffer.DEFAULT_BUFFER_SIZE);
        byte[] bytes;
        try {
            bytes = ProtostuffIOUtil.toByteArray(message, schema, linkedBuffer);
        }catch (Exception e){
            throw new IllegalStateException(e);
        }finally {
            linkedBuffer.clear();
        }
        return bytes;
    }
}
