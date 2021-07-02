package cn.ttplatform.wh.data.snapshot;

import cn.ttplatform.wh.data.support.SnapshotFileMetadataRegion;
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
    private SnapshotHeader snapshotHeader;
    private final Pool<ByteBuffer> byteBufferPool;
    private final SnapshotFileMetadataRegion snapshotFileMetadataRegion;

    public Snapshot(File file, SnapshotFileMetadataRegion snapshotFileMetadataRegion, Pool<ByteBuffer> byteBufferPool) {
        this.snapshotFileMetadataRegion = snapshotFileMetadataRegion;
        this.fileOperator = new SyncFileOperator(file, byteBufferPool, snapshotFileMetadataRegion);
        this.byteBufferPool = byteBufferPool;
        initialize();
    }

    public void initialize() {
        if (!fileOperator.isEmpty()) {
            snapshotHeader = SnapshotHeader.builder()
                    .fileSize(snapshotFileMetadataRegion.getFileSize())
                    .lastIncludeIndex(snapshotFileMetadataRegion.getLastIncludeIndex())
                    .lastIncludeTerm(snapshotFileMetadataRegion.getLastIncludeTerm())
                    .build();
        } else {
            snapshotHeader = new SnapshotHeader();
        }
    }

    public int getLastIncludeIndex() {
        return snapshotHeader.getLastIncludeIndex();
    }

    public int getLastIncludeTerm() {
        return snapshotHeader.getLastIncludeTerm();
    }

    public byte[] read(long offset, int length) {
        if (fileOperator.isEmpty()) {
            log.debug("snapshot file is empty.");
            return new byte[0];
        }
        long fileSize = fileOperator.size();
        if (offset > fileSize) {
            throw new IllegalStateException("offset[" + offset + "] out of bound[" + fileSize + "]");
        }
        length = Math.min(length, (int) (fileSize - offset));
        return fileOperator.readBytes(offset, length);
    }

    public ByteBuffer read() {
        int fileSize = (int) snapshotHeader.getFileSize();
        ByteBuffer byteBuffer = byteBufferPool.allocate(fileSize);
        fileOperator.readBytes(0, byteBuffer, fileSize);
        return byteBuffer;
    }

    public boolean isEmpty() {
        return fileOperator.isEmpty();
    }

    public long size() {
        return fileOperator.size();
    }

    public void clear() {
        fileOperator.truncate(SnapshotHeader.BYTES);
        snapshotHeader.reset();
    }

    public void close() {
        fileOperator.close();
    }

}
