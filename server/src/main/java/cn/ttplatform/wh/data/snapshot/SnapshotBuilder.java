package cn.ttplatform.wh.data.snapshot;

import cn.ttplatform.wh.data.FileConstant;
import cn.ttplatform.wh.data.support.SyncFileOperator;
import cn.ttplatform.wh.exception.OperateFileException;
import cn.ttplatform.wh.support.Pool;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;

import lombok.extern.slf4j.Slf4j;

import static cn.ttplatform.wh.data.snapshot.Snapshot.SnapshotHeader.*;

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

    public SnapshotBuilder(File parent, Pool<ByteBuffer> byteBufferPool) {
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
            throw new OperateFileException("delete or create file error.", e);
        }
        this.fileOperator = new SyncFileOperator(file, byteBufferPool, Snapshot.SnapshotHeader.BYTES);
    }

    public long getInstallOffset() {
        return fileOperator.size();
    }

    public void append(byte[] chunk) {
        if (chunk.length == 0) {
            log.debug("chunk size is 0.");
            return;
        }
        fileOperator.append(chunk, chunk.length);
    }

    public void append(ByteBuffer chunk, int length) {
        fileOperator.append(chunk, length);
    }

    public void writeHeader(long size, int lastIncludeIndex, int lastIncludeTerm) {
        fileOperator.writeHeader(FILE_SIZE_FIELD_POSITION,size);
        fileOperator.writeHeader(LAST_INCLUDE_INDEX_FIELD_POSITION,lastIncludeIndex);
        fileOperator.writeHeader(LAST_INCLUDE_TERM_FIELD_POSITION,lastIncludeTerm);
    }

    public File getFile() {
        return file;
    }

    public String getSnapshotSource() {
        return snapshotSource;
    }

    public void complete() {
        fileOperator.writeHeader(LAST_INCLUDE_INDEX_FIELD_POSITION, lastIncludeIndex);
        fileOperator.writeHeader(LAST_INCLUDE_TERM_FIELD_POSITION, lastIncludeTerm);
        fileOperator.close();
    }
}
