package cn.ttplatform.wh.core.group;

import cn.ttplatform.wh.support.BufferPool;
import cn.ttplatform.wh.support.Factory;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

/**
 * @author Wang Hao
 * @date 2021/4/28 10:28
 */
class OldNewConfigFactory implements Factory<OldNewConfig> {

    private final BufferPool<LinkedBuffer> pool;
    private final Schema<OldNewConfig> schema = RuntimeSchema.getSchema(OldNewConfig.class);

    public OldNewConfigFactory(BufferPool<LinkedBuffer> pool) {
        this.pool = pool;
    }

    @Override
    public OldNewConfig create(byte[] content) {
        OldNewConfig oldNewConfig = new OldNewConfig();
        ProtostuffIOUtil.mergeFrom(content, oldNewConfig, schema);
        return oldNewConfig;
    }

    @Override
    public byte[] getBytes(OldNewConfig oldNewConfig) {
        return ProtostuffIOUtil.toByteArray(oldNewConfig, schema, pool.allocate());
    }
}
