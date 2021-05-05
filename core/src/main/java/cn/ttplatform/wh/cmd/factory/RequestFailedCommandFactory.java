package cn.ttplatform.wh.cmd.factory;

import cn.ttplatform.wh.cmd.RequestFailedCommand;
import cn.ttplatform.wh.constant.DistributableType;
import cn.ttplatform.wh.support.AbstractDistributableFactory;
import cn.ttplatform.wh.support.Pool;
import cn.ttplatform.wh.support.Distributable;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

/**
 * @author Wang Hao
 * @date 2021/4/24 20:37
 */
public class RequestFailedCommandFactory extends AbstractDistributableFactory {

    private final Schema<RequestFailedCommand> schema = RuntimeSchema.getSchema(RequestFailedCommand.class);

    public RequestFailedCommandFactory(Pool<LinkedBuffer> pool) {
        super(pool);
    }

    @Override
    public int getFactoryType() {
        return DistributableType.REQUEST_FAILED_COMMAND;
    }

    @Override
    public Distributable create(byte[] content, int length) {
        RequestFailedCommand command = new RequestFailedCommand();
        ProtostuffIOUtil.mergeFrom(content, 0, length, command, schema);
        return command;
    }

    @Override
    public byte[] getBytes(Distributable distributable, LinkedBuffer buffer) {
        return ProtostuffIOUtil.toByteArray((RequestFailedCommand) distributable, schema, buffer);
    }

}
