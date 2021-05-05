package cn.ttplatform.wh.core.log.snapshot;

import static cn.ttplatform.wh.core.log.tool.ByteConvertor.fillIntBytes;
import static cn.ttplatform.wh.core.log.tool.ByteConvertor.fillLongBytes;

import cn.ttplatform.wh.core.log.generation.FileName;
import cn.ttplatform.wh.core.log.entry.LogEntryFactory;
import cn.ttplatform.wh.support.Pool;
import cn.ttplatform.wh.core.log.tool.ByteBufferWriter;
import cn.ttplatform.wh.core.log.tool.ReadableAndWriteableFile;
import java.io.File;
import java.nio.ByteBuffer;
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

    public FileSnapshot(File parent, Pool<ByteBuffer> byteBufferPool, Pool<byte[]> byteArrayPool, boolean isOldGeneration) {
        this.file = new ByteBufferWriter(new File(parent, FileName.SNAPSHOT_FILE_NAME), byteBufferPool, byteArrayPool);
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
            byte[] header;
            try {
                header = file.readBytesAt(0L, HEADER_LENGTH);
            } catch (Exception e) {
                file.clear();
                snapshotHeader.reset();
                return;
            }
            snapshotHeader = logEntryFactory.transferBytesToSnapshotHeader(header);
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
        byte[] header = byteArrayPool.allocate(FileSnapshot.HEADER_LENGTH);
        transferSnapshotHeaderToBytes(snapshotHeader, header);
        try {
            file.clear();
            file.append(header, FileSnapshot.HEADER_LENGTH);
            file.append(content, contentLength);
            snapshotHeader = newSnapshotHeader;
        } finally {
            byteArrayPool.recycle(header);
        }

    }

    /**
     * Convert a {@link SnapshotHeader} object to byte array
     *
     * @param snapshotHeader source object
     */
    public void transferSnapshotHeaderToBytes(SnapshotHeader snapshotHeader, byte[] header) {
        fillLongBytes(snapshotHeader.getSize(), header, 7);
        fillIntBytes(snapshotHeader.getLastIncludeIndex(), header, 11);
        fillIntBytes(snapshotHeader.getLastIncludeTerm(), header, 15);
    }

    public byte[] read(long offset, int size) {
        return file.readBytesAt(offset, size);
    }

    public byte[] readAll() {
        return file.readBytesAt(HEADER_LENGTH, (int) snapshotHeader.getContentLength());
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
