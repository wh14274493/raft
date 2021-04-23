package cn.ttplatform.wh.core.log.snapshot;

import cn.ttplatform.wh.constant.FileName;
import cn.ttplatform.wh.core.log.entry.LogFactory;
import cn.ttplatform.wh.support.BufferPool;
import cn.ttplatform.wh.core.support.ByteBufferWriter;
import cn.ttplatform.wh.core.support.ReadableAndWriteableFile;
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
    private final LogFactory logFactory = LogFactory.getInstance();
    private SnapshotHeader snapshotHeader;

    public FileSnapshot(File parent, BufferPool<ByteBuffer> pool, boolean isOldGeneration) {
        this.file = new ByteBufferWriter(new File(parent, FileName.SNAPSHOT_FILE_NAME), pool);
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
            snapshotHeader = logFactory.transferBytesToSnapshotHeader(header);
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
        byte[] header = logFactory.transferSnapshotHeaderToBytes(newSnapshotHeader);
        file.clear();
        file.writeBytesAt(0L, header);
        file.append(content);
        snapshotHeader = newSnapshotHeader;
    }

    public byte[] read(long offset, int size) {
        return file.readBytesAt(offset, size);
    }

    public byte[] readAll() {
        return file.readBytesAt(HEADER_LENGTH, (int) snapshotHeader.getContentLength());
    }

    public void append(byte[] content) {
        file.append(content);
    }

    public void clear() {
        file.clear();
    }

    public void close() {
        file.close();
    }

}
