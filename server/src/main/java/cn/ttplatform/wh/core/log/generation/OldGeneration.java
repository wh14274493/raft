package cn.ttplatform.wh.core.log.generation;

import cn.ttplatform.wh.support.BufferPool;
import java.io.File;
import java.nio.ByteBuffer;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Wang Hao
 * @date 2021/2/5 13:57
 */
@Slf4j
public class OldGeneration extends AbstractGeneration {

    public OldGeneration(File file, BufferPool<ByteBuffer> pool) {
        super(file, pool, true);
    }

    public byte[] readSnapshot(long offset, int size) {
        if (fileSnapshot.isEmpty()) {
            log.debug("snapshot file is empty.");
            return new byte[0];
        }
        return fileSnapshot.read(offset, size);
    }

    public byte[] readSnapshot() {
        return fileSnapshot.readAll();
    }
}
