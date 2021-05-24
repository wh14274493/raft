package cn.ttplatform.wh.core.data.snapshot;

import cn.ttplatform.wh.core.data.FileName;
import cn.ttplatform.wh.core.data.tool.PooledByteBuffer;
import cn.ttplatform.wh.support.Pool;
import java.io.File;
import java.util.Arrays;
import java.util.Optional;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Wang Hao
 * @date 2021/5/17 16:24
 */
@Slf4j
public class Snapshot {

    private final SnapshotFile snapshotFile;

    public Snapshot(File file, Pool<PooledByteBuffer> byteBufferPool, Pool<byte[]> byteArrayPool) {
        this.snapshotFile = new SnapshotFile(file, byteBufferPool, byteArrayPool);
    }

    public int getLastIncludeIndex() {
        return snapshotFile.getSnapshotHeader().getLastIncludeIndex();
    }

    public int getLastIncludeTerm() {
        return snapshotFile.getSnapshotHeader().getLastIncludeTerm();
    }

    public byte[] read(long offset, long size) {
        if (snapshotFile.isEmpty()) {
            log.debug("snapshot file is empty.");
            return new byte[0];
        }
        long fileSize = snapshotFile.size();
        if (offset > fileSize) {
            throw new IllegalStateException("offset[" + offset + "] out of bound[" + fileSize + "]");
        }
        size = Math.min(size, fileSize - offset);
        return snapshotFile.read(offset, (int) size);
    }

    public PooledByteBuffer readAll() {
        return snapshotFile.readAll();
    }

    public long size() {
        return snapshotFile.size();
    }

    public void delete() {
        snapshotFile.delete();
    }

    public void close() {
        snapshotFile.close();
    }

}
