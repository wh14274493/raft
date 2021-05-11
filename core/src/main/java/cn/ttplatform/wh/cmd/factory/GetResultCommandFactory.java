package cn.ttplatform.wh.cmd.factory;

import cn.ttplatform.wh.cmd.GetResultCommand;
import cn.ttplatform.wh.constant.DistributableType;
import cn.ttplatform.wh.constant.ErrorMessage;
import cn.ttplatform.wh.exception.MessageParseException;
import cn.ttplatform.wh.support.AbstractDistributableFactory;
import cn.ttplatform.wh.support.Distributable;
import cn.ttplatform.wh.support.Pool;
import io.netty.buffer.ByteBuf;
import io.protostuff.ByteBufferInput;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * @author Wang Hao
 * @date 2021/4/16 22:49
 */
public class GetResultCommandFactory extends AbstractDistributableFactory {

    private final Schema<GetResultCommand> schema = RuntimeSchema.getSchema(GetResultCommand.class);

    public GetResultCommandFactory(Pool<LinkedBuffer> pool) {
        super(pool);
    }

    @Override
    public int getFactoryType() {
        return DistributableType.GET_COMMAND_RESULT;
    }

    @Override
    public Distributable create(byte[] content, int length) {
        GetResultCommand command = new GetResultCommand();
        ProtostuffIOUtil.mergeFrom(content, 0, length, command, schema);
        return command;
    }

    @Override
    public Distributable create(ByteBuffer byteBuffer) {
        GetResultCommand cmd = new GetResultCommand();
        try {
            schema.mergeFrom(new ByteBufferInput(byteBuffer, true), cmd);
        } catch (IOException e) {
            throw new MessageParseException(ErrorMessage.MESSAGE_PARSE_ERROR);
        }
        return cmd;
    }

    @Override
    public byte[] getBytes(Distributable distributable, LinkedBuffer buffer) {
        return ProtostuffIOUtil.toByteArray((GetResultCommand) distributable, schema, buffer);
    }

    @Override
    public void getBytes(Distributable distributable, LinkedBuffer buffer, ByteBuf byteBuffer, OutputStream outputStream)
        throws IOException {
        ProtostuffIOUtil.writeTo(outputStream, (GetResultCommand) distributable, schema, buffer);
    }
}
