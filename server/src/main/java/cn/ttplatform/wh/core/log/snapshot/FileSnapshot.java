package cn.ttplatform.wh.core.log.snapshot;

import cn.ttplatform.wh.core.log.generation.FileName;
import cn.ttplatform.wh.core.log.entry.LogEntryFactory;
import cn.ttplatform.wh.core.log.tool.PooledByteBuffer;
import cn.ttplatform.wh.support.Pool;
import cn.ttplatform.wh.core.log.tool.ByteBufferWriter;
import cn.ttplatform.wh.core.log.tool.ReadableAndWriteableFile;
import java.io.File;
import lombok.Getter;
import lombok.Setter;

/**
 * @author Wang Hao
 * @date 2021/2/4 23:34
 */
@Setter
@Getter
public class FileSnapshot {

    public static final int HEADER_LENGTH = 16;
    private final ReadableAndWriteableFile file;
    private final LogEntryFactory logEntryFactory = LogEntryFactory.getInstance();
    private SnapshotHeader snapshotHeader;
    private final Pool<byte[]> byteArrayPool;
    private final Pool<PooledByteBuffer> byteBufferPool;

    public FileSnapshot(File parent, Pool<PooledByteBuffer> byteBufferPool, Pool<byte[]> byteArrayPool, boolean isOldGeneration) {
        this.file = new ByteBufferWriter(new File(parent, FileName.SNAPSHOT_FILE_NAME), byteBufferPool, byteArrayPool);
        this.byteBufferPool = byteBufferPool;
        this.byteArrayPool = byteArrayPool;
        if (isOldGeneration) {
            initialize();
        } else {
            snapshotHeader = new SnapshotHeader();
            file.clear();
        }
    }

    public void initialize() {
        if (!file.isEmpty()) {
            PooledByteBuffer header;
            try {
                header = file.readByteBufferAt(0L, HEADER_LENGTH);
            } catch (Exception e) {
                file.clear();
                snapshotHeader.reset();
                return;
            }
            header.flip();
            snapshotHeader = SnapshotHeader.builder().size(header.getLong()).lastIncludeIndex(header.getInt())
                .lastIncludeTerm(header.getInt())
                .build();
            header.recycle();
            if (snapshotHeader.getSize() != file.size()) {
                file.clear();
                snapshotHeader.reset();
            }
        } else {
            snapshotHeader = new SnapshotHeader();
        }
    }

    public void write(int lastIncludeIndex, int lastIncludeTerm, byte[] content) {
        int contentLength = content.length;
        int size = contentLength + HEADER_LENGTH;
        SnapshotHeader newSnapshotHeader = SnapshotHeader.builder().lastIncludeIndex(lastIncludeIndex)
            .lastIncludeTerm(lastIncludeTerm).size(size).contentLength(contentLength).build();
        PooledByteBuffer byteBuffer = byteBufferPool.allocate(size);
        try {
            file.clear();
            byteBuffer.putLong(newSnapshotHeader.getSize());
            byteBuffer.putInt(newSnapshotHeader.getLastIncludeIndex());
            byteBuffer.putInt(newSnapshotHeader.getLastIncludeTerm());
            byteBuffer.put(content);
            file.append(byteBuffer, size);
            snapshotHeader = newSnapshotHeader;
        } finally {
            byteBufferPool.recycle(byteBuffer);
        }

    }

    public byte[] read(long offset, int size) {
        return file.readBytesAt(offset, size);
    }

    public PooledByteBuffer readAll() {
        return file.readByteBufferAt(HEADER_LENGTH, (int) snapshotHeader.getContentLength());
    }

    public void append(byte[] content) {
        file.append(content, content.length);
    }

    public boolean isEmpty() {
        return file.isEmpty();
    }

    public long size() {
        return file.size();
    }

    public void clear() {
        file.clear();
    }

    public void close() {
        file.close();
    }

}
