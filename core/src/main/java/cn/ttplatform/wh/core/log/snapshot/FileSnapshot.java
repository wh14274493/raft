package cn.ttplatform.wh.core.log.snapshot;

import cn.ttplatform.wh.constant.FileName;
import cn.ttplatform.wh.core.common.RandomAccessFileWrapper;
import java.io.File;
import lombok.Getter;

/**
 * @author Wang Hao
 * @date 2021/2/4 23:34
 */
@Getter
public class FileSnapshot {

    private static final long HEADER_LENGTH = 16L;
    private final RandomAccessFileWrapper file;
    private int lastIncludeIndex;
    private int lastIncludeTerm;
    private long size;
    private long contentLength;

    public FileSnapshot(File parent) {
        this.file = new RandomAccessFileWrapper(new File(parent, FileName.SNAPSHOT_FILE_NAME));
        if (!file.isEmpty()) {
            size = file.readLong();
            if (size != file.size()) {
                file.clear();
                size = 0L;
                return;
            }
            lastIncludeIndex = file.readInt();
            lastIncludeTerm = file.readInt();
        }
    }

    public void write(int lastIncludeIndex, int lastIncludeTerm, byte[] content) {
        this.contentLength = content.length;
        this.size = contentLength + HEADER_LENGTH;
        file.clear();
        file.writeLong(size);
        file.writeInt(lastIncludeIndex);
        file.writeInt(lastIncludeTerm);
        file.writeBytes(content);
        this.lastIncludeIndex = lastIncludeIndex;
        this.lastIncludeTerm = lastIncludeTerm;
    }

    public byte[] read(long offset, int size) {
        file.seek(offset);
        return file.readBytes(size);
    }

    public byte[] readAll() {
        file.seek(HEADER_LENGTH);
        return file.readBytes((int) contentLength);
    }

    public void append(byte[] content) {
        file.append(content);
    }

    public void close() {
        file.close();
    }

}
