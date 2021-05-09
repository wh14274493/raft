package cn.ttplatform.wh.core.log.generation;

import cn.ttplatform.wh.support.PooledByteBuffer;
import cn.ttplatform.wh.support.Pool;
import java.io.File;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Wang Hao
 * @date 2021/2/5 13:57
 */
@Slf4j
public class OldGeneration extends AbstractGeneration {

    public OldGeneration(File file, Pool<PooledByteBuffer> pool, Pool<byte[]> byteArrayPool) {
        super(file, pool, byteArrayPool, true);
    }

    public byte[] readSnapshot(long offset, long size) {
        if (fileSnapshot.isEmpty()) {
            log.debug("snapshot file is empty.");
            return new byte[0];
        }
        long fileSize = fileSnapshot.size();
        if (offset > fileSize) {
            throw new IllegalStateException("offset[" + offset + "] out of bound[" + fileSize + "]");
        }
        size = Math.min(size, fileSize - offset);
        return fileSnapshot.read(offset, (int) size);
    }

    public PooledByteBuffer readSnapshot() {
        return fileSnapshot.readAll();
    }
}
