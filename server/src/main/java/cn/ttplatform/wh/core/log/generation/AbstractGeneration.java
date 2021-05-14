package cn.ttplatform.wh.core.log.generation;

import cn.ttplatform.wh.core.log.entry.FileLogEntry;
import cn.ttplatform.wh.core.log.entry.FileLogEntryIndex;
import cn.ttplatform.wh.core.log.snapshot.FileSnapshot;
import cn.ttplatform.wh.core.log.tool.PooledByteBuffer;
import cn.ttplatform.wh.exception.OperateFileException;
import cn.ttplatform.wh.support.Pool;
import java.io.File;

/**
 * @author Wang Hao
 * @date 2021/2/14 16:23
 */
public abstract class AbstractGeneration implements Generation {

    protected File file;
    protected FileSnapshot fileSnapshot;
    protected FileLogEntry fileLogEntry;
    protected FileLogEntryIndex fileLogEntryIndex;

    AbstractGeneration(File file, Pool<PooledByteBuffer> pool, Pool<byte[]> byteArrayPool, boolean isOldGeneration) {
        if (!file.exists() && !file.mkdir()) {
            throw new OperateFileException("create file[" + file.getPath() + "] error");
        }
        this.file = file;
        this.fileSnapshot = new FileSnapshot(file, pool, byteArrayPool, isOldGeneration);
    }

    public int getLastIncludeIndex() {
        return fileSnapshot.getSnapshotHeader().getLastIncludeIndex();
    }

    public int getLastIncludeTerm() {
        return fileSnapshot.getSnapshotHeader().getLastIncludeTerm();
    }

    public long getSnapshotSize() {
        return fileSnapshot.size();
    }

    public long getLogEntryFileSize() {
        return fileLogEntry.size();
    }

    @Override
    public void close() {
        fileSnapshot.close();
    }
}
