package cn.ttplatform.wh.core.log.snapshot;

import cn.ttplatform.wh.support.RandomAccessFileWrapper;
import java.io.File;

/**
 * @author Wang Hao
 * @date 2021/2/4 23:34
 */
public class FileSnapshot {

    private final RandomAccessFileWrapper file;
    private int lastIncludeIndex;
    private int lastIncludeTerm;
    private long contentLength;
    private long size;

    public FileSnapshot(File file) {
        this.file = new RandomAccessFileWrapper(file);
        initialize();
    }

    private void initialize() {
        if (file.size() != 0) {
            lastIncludeIndex = file.readInt();
            lastIncludeTerm = file.readInt();
            contentLength = file.readLong();
            size = file.size();
        }
    }

    public void rebuild(int lastIncludeIndex, int lastIncludeTerm, byte[] content) {
        this.lastIncludeIndex = lastIncludeIndex;
        this.lastIncludeTerm = lastIncludeTerm;
        this.contentLength = content.length;
        file.seek(0L);
        file.writeInt(lastIncludeIndex);
        file.writeInt(lastIncludeTerm);
        file.writeLong(contentLength);
        file.writeBytes(content);
        this.size = file.size();
    }

    public int getLastIncludeIndex() {
        return lastIncludeIndex;
    }

    public int getLastIncludeTerm() {
        return lastIncludeTerm;
    }

    public long getSize() {
        return size;
    }

    public byte[] read(long offset, int size) {
        file.seek(offset);
        return file.readBytes(size);
    }

    public void write(byte[] content) {
        file.append(content);
    }

    public void close() {
        file.close();
    }
}
