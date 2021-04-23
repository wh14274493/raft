package cn.ttplatform.wh.core.log;

import cn.ttplatform.wh.core.log.entry.FileLogEntry;
import cn.ttplatform.wh.core.log.entry.FileLogEntryIndex;
import cn.ttplatform.wh.core.log.snapshot.FileSnapshot;
import cn.ttplatform.wh.support.BufferPool;
import cn.ttplatform.wh.exception.OperateFileException;
import java.io.File;
import java.nio.ByteBuffer;

/**
 * @author Wang Hao
 * @date 2021/2/14 16:23
 */
public abstract class AbstractGeneration implements Generation {

    File file;
    FileSnapshot fileSnapshot;
    FileLogEntry fileLogEntry;
    FileLogEntryIndex fileLogEntryIndex;

    AbstractGeneration(File file, BufferPool<ByteBuffer> pool,boolean isOldGeneration) {
        if (!file.exists() && !file.mkdir()) {
            throw new OperateFileException("create file[" + file.getPath() + "] error");
        }
        this.file = file;
        this.fileSnapshot = new FileSnapshot(file, pool,isOldGeneration);
    }

    public int getLastIncludeIndex() {
        return fileSnapshot.getSnapshotHeader().getLastIncludeIndex();
    }

    public int getLastIncludeTerm() {
        return fileSnapshot.getSnapshotHeader().getLastIncludeTerm();
    }

    public long getSnapshotSize() {
        return fileSnapshot.getSnapshotHeader().getSize();
    }

    public long getLogEntryFileSize() {
        return fileLogEntry.size();
    }

    @Override
    public void close() {
        fileSnapshot.close();
    }
}
