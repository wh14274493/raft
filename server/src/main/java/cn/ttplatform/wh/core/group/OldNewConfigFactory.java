package cn.ttplatform.wh.core.group;

import cn.ttplatform.wh.support.Pool;
import cn.ttplatform.wh.support.Factory;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;
import java.nio.ByteBuffer;

/**
 * @author Wang Hao
 * @date 2021/4/28 10:28
 */
class OldNewConfigFactory implements Factory<OldNewConfig> {

    private final Pool<LinkedBuffer> pool;
    private final Schema<OldNewConfig> schema = RuntimeSchema.getSchema(OldNewConfig.class);

    public OldNewConfigFactory(Pool<LinkedBuffer> pool) {
        this.pool = pool;
    }

    @Override
    public OldNewConfig create(byte[] content, int length) {
        OldNewConfig oldNewConfig = new OldNewConfig();
        ProtostuffIOUtil.mergeFrom(content, 0, length, oldNewConfig, schema);
        return oldNewConfig;
    }

    @Override
    public OldNewConfig create(ByteBuffer byteBuffer, int contentLength) {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte[] getBytes(OldNewConfig oldNewConfig) {
        return ProtostuffIOUtil.toByteArray(oldNewConfig, schema, pool.allocate());
    }
}
