package cn.ttplatform.wh.cmd.factory;

import cn.ttplatform.wh.cmd.RedirectCommand;
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
 * @date 2021/4/23 23:18
 */
public class RedirectCommandFactory extends AbstractDistributableFactory {

    private final Schema<RedirectCommand> schema = RuntimeSchema.getSchema(RedirectCommand.class);

    public RedirectCommandFactory(Pool<LinkedBuffer> pool) {
        super(pool);
    }

    @Override
    public int getFactoryType() {
        return DistributableType.REDIRECT_COMMAND;
    }

    @Override
    public byte[] getBytes(Distributable distributable, LinkedBuffer buffer) {
        return ProtostuffIOUtil.toByteArray((RedirectCommand) distributable, schema, buffer);
    }

    @Override
    public Distributable create(byte[] content, int length) {
        RedirectCommand command = new RedirectCommand();
        ProtostuffIOUtil.mergeFrom(content, 0, length, command, schema);
        return command;
    }
}
