package cn.ttplatform.wh.core.log.snapshot;

import cn.ttplatform.wh.constant.FileName;
import cn.ttplatform.wh.core.support.DirectAccessFile;
import cn.ttplatform.wh.core.support.DirectByteBufferPool;
import cn.ttplatform.wh.core.support.RandomAccessFileWrapper;
import cn.ttplatform.wh.core.support.ReadableAndWriteableFile;
import cn.ttplatform.wh.core.log.LogFactory;
import java.io.File;
import lombok.Getter;

/**
 * @author Wang Hao
 * @date 2021/2/4 23:34
 */
@Getter
public class FileSnapshot {

    public static final int HEADER_LENGTH = 16;
    private final ReadableAndWriteableFile file;
    private final LogFactory logFactory = LogFactory.getInstance();
    private SnapshotHeader snapshotHeader;

    public FileSnapshot(File parent) {
        this.file = new RandomAccessFileWrapper(new File(parent, FileName.SNAPSHOT_FILE_NAME));
        initialize();
    }

    public FileSnapshot(File parent, DirectByteBufferPool pool) {
        this.file = new DirectAccessFile(new File(parent, FileName.SNAPSHOT_FILE_NAME), pool);
        initialize();
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
            snapshotHeader = logFactory.transferBytesToSnapshotHeader(header, 0);
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

    public void close() {
        file.close();
    }

}
