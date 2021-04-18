package cn.ttplatform.wh.core.log;

import java.io.File;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Wang Hao
 * @date 2021/2/5 13:57
 */
@Slf4j
public class OldGeneration extends AbstractGeneration {

    public OldGeneration(File file) {
        super(file);
    }

    public byte[] readSnapshot(long offset, int size) {
        return fileSnapshot.read(offset, size);
    }

    public byte[] readSnapshot() {
        return fileSnapshot.readAll();
    }
}
