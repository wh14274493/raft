package cn.ttplatform.wh.core.data.snapshot;

import cn.ttplatform.wh.constant.ErrorMessage;
import cn.ttplatform.wh.core.data.FileName;
import cn.ttplatform.wh.core.data.tool.ByteBufferWriter;
import cn.ttplatform.wh.core.data.tool.PooledByteBuffer;
import cn.ttplatform.wh.core.data.tool.ReadableAndWriteableFile;
import cn.ttplatform.wh.exception.OperateFileException;
import cn.ttplatform.wh.support.Pool;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Wang Hao
 * @date 2021/5/17 16:30
 */
@Slf4j
public class SnapshotBuilder {

    private File file;
    private String snapshotSource;
    private int lastIncludeIndex;
    private int lastIncludeTerm;
    private ReadableAndWriteableFile generatingFile;
    private final File parent;
    private final Pool<PooledByteBuffer> byteBufferPool;

    public SnapshotBuilder(File parent, Pool<PooledByteBuffer> byteBufferPool) {
        this.byteBufferPool = byteBufferPool;
        this.parent = parent;
    }

    public void setBaseInfo(int lastIncludeIndex, int lastIncludeTerm, String snapshotSource) {
        this.lastIncludeIndex = lastIncludeIndex;
        this.lastIncludeTerm = lastIncludeTerm;
        this.snapshotSource = snapshotSource;
        this.file = new File(parent, String.format(FileName.SNAPSHOT_GENERATING_FILE_NAME, lastIncludeTerm, lastIncludeIndex));
        try {
            Files.deleteIfExists(file.toPath());
            Files.createFile(file.toPath());
        } catch (IOException e) {
            throw new OperateFileException("delete or create file error.", e);
        }
        this.generatingFile = new ByteBufferWriter(file, byteBufferPool);
    }

    public long getInstallOffset() {
        return generatingFile.size();
    }

    public void resetInstallOffset() {
        generatingFile.clear();
    }

    public void append(byte[] chunk) {
        if (chunk.length == 0) {
            log.debug("chunk size is 0.");
            return;
        }
        generatingFile.append(chunk, chunk.length);
    }

    public void append(PooledByteBuffer chunk, int length) {
        generatingFile.append(chunk, length);
    }

    public File getFile() {
        return file;
    }

    public String getSnapshotSource() {
        return snapshotSource;
    }

    public void complete() {
        generatingFile.close();
    }
}
