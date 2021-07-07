package cn.ttplatform.wh.data.snapshot;

import cn.ttplatform.wh.data.FileConstant;
import cn.ttplatform.wh.data.support.SyncFileOperator;
import cn.ttplatform.wh.exception.OperateFileException;
import cn.ttplatform.wh.support.Pool;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;

/**
 * @author Wang Hao
 * @date 2021/5/17 16:30
 */
@Slf4j
public class SnapshotBuilder {

    private File file;
    private String snapshotSource;
    private SyncFileOperator fileOperator;
    private int lastIncludeIndex;
    private int lastIncludeTerm;
    private final File parent;
    private final Pool<ByteBuffer> byteBufferPool;
    private final SnapshotFileMetadataRegion snapshotFileMetadataRegion;
    private final SnapshotFileMetadataRegion generatingSnapshotFileMetadataRegion;

    public SnapshotBuilder(File parent, Pool<ByteBuffer> byteBufferPool, SnapshotFileMetadataRegion snapshotFileMetadataRegion,
                           SnapshotFileMetadataRegion generatingSnapshotFileMetadataRegion) {
        this.snapshotFileMetadataRegion = snapshotFileMetadataRegion;
        this.generatingSnapshotFileMetadataRegion = generatingSnapshotFileMetadataRegion;
        this.byteBufferPool = byteBufferPool;
        this.parent = parent;
    }

    public void setBaseInfo(int lastIncludeIndex, int lastIncludeTerm, String snapshotSource) {
        this.lastIncludeIndex = lastIncludeIndex;
        this.lastIncludeTerm = lastIncludeTerm;
        this.snapshotSource = snapshotSource;
        this.file = FileConstant.newSnapshotFile(parent, lastIncludeIndex, lastIncludeTerm);
        try {
            Files.deleteIfExists(file.toPath());
            Files.createFile(file.toPath());
        } catch (IOException e) {
            throw new OperateFileException("failed to delete or create file.", e);
        }
        generatingSnapshotFileMetadataRegion.clear();
        this.fileOperator = new SyncFileOperator(file, byteBufferPool);
    }

    public long getInstallOffset() {
        return generatingSnapshotFileMetadataRegion.getFileSize();
    }

    public void append(byte[] chunk) {
        if (chunk.length == 0) {
            log.debug("chunk size is 0.");
            return;
        }
        long fileSize = generatingSnapshotFileMetadataRegion.getFileSize();
        fileOperator.append(fileSize, chunk, chunk.length);
        generatingSnapshotFileMetadataRegion.recordFileSize(fileSize + chunk.length);
    }

    public File getFile() {
        return file;
    }

    public String getSnapshotSource() {
        return snapshotSource;
    }

    public void complete() {
        snapshotFileMetadataRegion.recordFileSize(generatingSnapshotFileMetadataRegion.getFileSize());
        snapshotFileMetadataRegion.recordLastIncludeIndex(lastIncludeIndex);
        snapshotFileMetadataRegion.recordLastIncludeTerm(lastIncludeTerm);
        fileOperator.close();
    }
}
