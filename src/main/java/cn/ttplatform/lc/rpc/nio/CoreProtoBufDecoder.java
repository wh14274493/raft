package cn.ttplatform.lc.rpc.nio;

import cn.ttplatform.lc.constant.RpcMessageType;
import cn.ttplatform.lc.exception.UnknownTypeException;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import java.util.List;

/**
 * @author : wang hao
 * @description : CoreProtoBufDecoder
 * @date :  2020/8/16 0:17
 **/
public class CoreProtoBufDecoder extends ByteToMessageDecoder {

    private static final int MESSAGE_HEADER_LENGTH = Integer.BYTES + Long.BYTES;

    /**
     * Decode the from one {@link ByteBuf} to an other. This method will be called till either the input {@link ByteBuf}
     * has nothing to read when return from this method or till nothing was read from the input {@link ByteBuf}.
     *
     * @param ctx the {@link ChannelHandlerContext} which this {@link ByteToMessageDecoder} belongs to
     * @param in  the {@link ByteBuf} from which to read data
     * @param out the {@link List} to which decoded messages should be added
     * @throws Exception is thrown if an error occurs
     */
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {

        if (in.readableBytes() < MESSAGE_HEADER_LENGTH) {
            return;
        }
        in.markReaderIndex();
        int type = in.readInt();
        long contentLength = in.readLong();
        if (in.readableBytes() < contentLength) {
            in.resetReaderIndex();
            return;
        }
        byte[] content = new byte[(int) contentLength];
        in.readBytes(content);
        switch (type) {
            case RpcMessageType.APPEND_LOG_ENTRIES:
                break;
            default:
                throw new UnknownTypeException("unknown message type[" + type + "]");

        }
    }
}
