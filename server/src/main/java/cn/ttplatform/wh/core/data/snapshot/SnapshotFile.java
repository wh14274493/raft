package cn.ttplatform.wh.core.data.snapshot;

import cn.ttplatform.wh.core.data.tool.ByteBufferWriter;
import cn.ttplatform.wh.core.data.tool.PooledByteBuffer;
import cn.ttplatform.wh.core.data.tool.ReadableAndWriteableFile;
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
    private final Pool<byte[]> byteArrayPool;
    private final Pool<PooledByteBuffer> byteBufferPool;

    public SnapshotFile(File file, Pool<PooledByteBuffer> byteBufferPool, Pool<byte[]> byteArrayPool) {
        this.file = new ByteBufferWriter(file, byteBufferPool);
        this.byteBufferPool = byteBufferPool;
        this.byteArrayPool = byteArrayPool;
        initialize();
    }

    public void initialize() {
        if (!file.isEmpty()) {
            PooledByteBuffer header;
            try {
                header = file.readByteBufferAt(0L, HEADER_LENGTH);
            } catch (Exception e) {
                throw new SnapshotParseException("parse snapshot header error.");
            }
            header.flip();
            long contentLength = header.getLong();
            if (contentLength != file.size() - HEADER_LENGTH) {
                throw new SnapshotParseException("snapshot content is unmatched.");
            }
            snapshotHeader = SnapshotHeader.builder().contentLength(contentLength).lastIncludeIndex(header.getInt())
                .lastIncludeTerm(header.getInt())
                .build();
            header.recycle();
        } else {
            snapshotHeader = new SnapshotHeader();
        }
    }

    public byte[] read(long offset, int size) {
        return file.readBytesAt(offset, size);
    }

    public PooledByteBuffer readAll() {
        return file.readByteBufferAt(HEADER_LENGTH, (int) snapshotHeader.getContentLength());
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
