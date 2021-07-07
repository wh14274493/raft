package cn.ttplatform.wh.data.snapshot;

import cn.ttplatform.wh.data.support.SyncFileOperator;
import cn.ttplatform.wh.support.Pool;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.nio.ByteBuffer;

/**
 * @author Wang Hao
 * @date 2021/2/4 23:34
 */
@Setter
@Getter
@Slf4j
public class Snapshot {

    private final SyncFileOperator fileOperator;
    private final Pool<ByteBuffer> byteBufferPool;
    private final SnapshotFileMetadataRegion snapshotFileMetadataRegion;

    public Snapshot(File file, SnapshotFileMetadataRegion snapshotFileMetadataRegion, Pool<ByteBuffer> byteBufferPool) {
        this.snapshotFileMetadataRegion = snapshotFileMetadataRegion;
        this.fileOperator = new SyncFileOperator(file, byteBufferPool);
        this.byteBufferPool = byteBufferPool;
    }

    public int getLastIncludeIndex() {
        return snapshotFileMetadataRegion.getLastIncludeIndex();
    }

    public int getLastIncludeTerm() {
        return snapshotFileMetadataRegion.getLastIncludeTerm();
    }

    public byte[] read(long offset, int length) {
        if (isEmpty()) {
            log.debug("snapshot file is empty.");
            return new byte[0];
        }
        long fileSize = snapshotFileMetadataRegion.getFileSize();
        if (offset > fileSize) {
            throw new IllegalStateException(String.format("offset[%d] out of bound[%d].", offset, fileSize));
        }
        length = Math.min(length, (int) (fileSize - offset));
        return fileOperator.readBytes(offset, length);
    }

    public ByteBuffer read() {
        int fileSize = (int) snapshotFileMetadataRegion.getFileSize();
        ByteBuffer byteBuffer = byteBufferPool.allocate(fileSize);
        fileOperator.readBytes(0, byteBuffer, fileSize);
        return byteBuffer;
    }

    public boolean isEmpty() {
        return snapshotFileMetadataRegion.getFileSize() == 0;
    }

    public long size() {
        return snapshotFileMetadataRegion.getFileSize();
    }

    public void close() {
        fileOperator.close();
    }

}
