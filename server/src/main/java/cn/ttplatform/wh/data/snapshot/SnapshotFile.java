package cn.ttplatform.wh.data.snapshot;

import cn.ttplatform.wh.data.tool.ByteBufferWriter;
import cn.ttplatform.wh.data.tool.PooledByteBuffer;
import cn.ttplatform.wh.data.tool.ReadableAndWriteableFile;
import cn.ttplatform.wh.exception.SnapshotParseException;
import cn.ttplatform.wh.support.Pool;
import java.io.File;
import lombok.Getter;
import lombok.Setter;

/**
 * @author Wang Hao
 * @date 2021/2/4 23:34
 */
@Setter
@Getter
public class SnapshotFile {

    public static final int HEADER_LENGTH = 16;
    private final ReadableAndWriteableFile file;
    private SnapshotHeader snapshotHeader;
    private final Pool<PooledByteBuffer> byteBufferPool;

    public SnapshotFile(File file, Pool<PooledByteBuffer> byteBufferPool) {
        this.file = new ByteBufferWriter(file, byteBufferPool);
        this.byteBufferPool = byteBufferPool;
        initialize();
    }

    public void initialize() {
        if (!file.isEmpty()) {
            PooledByteBuffer byteBuffer = byteBufferPool.allocate(HEADER_LENGTH);
            try {
                file.readByteBufferAt(0L, byteBuffer, HEADER_LENGTH);
            } catch (Exception e) {
                throw new SnapshotParseException("parse snapshot header error.");
            }
            long contentLength = byteBuffer.getLong();
            if (contentLength != file.size() - HEADER_LENGTH) {
                throw new SnapshotParseException("snapshot content is unmatched.");
            }
            snapshotHeader = SnapshotHeader.builder().contentLength(contentLength).lastIncludeIndex(byteBuffer.getInt())
                .lastIncludeTerm(byteBuffer.getInt())
                .build();
            byteBuffer.recycle();
        } else {
            snapshotHeader = new SnapshotHeader();
        }
    }

    public byte[] read(long offset, int size) {
        return file.readBytesAt(offset, size);
    }

    public PooledByteBuffer readAll() {
        PooledByteBuffer byteBuffer = byteBufferPool.allocate((int) snapshotHeader.getContentLength());
        try {
            file.readByteBufferAt(HEADER_LENGTH, byteBuffer, (int) snapshotHeader.getContentLength());
        } finally {
            byteBufferPool.recycle(byteBuffer);
        }
        return byteBuffer;
    }

    public boolean isEmpty() {
        return file.isEmpty();
    }

    public long size() {
        return file.size();
    }

    public void delete() {
        file.delete();
    }

    public void clear() {
        file.clear();
    }

    public void close() {
        file.close();
    }

}
