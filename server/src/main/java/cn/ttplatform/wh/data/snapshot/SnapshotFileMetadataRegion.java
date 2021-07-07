package cn.ttplatform.wh.data.snapshot;

import cn.ttplatform.wh.data.support.MetadataRegion;

import java.io.File;

import static cn.ttplatform.wh.data.FileConstant.SNAPSHOT_FILE_HEADER_SPACE_POSITION;
import static cn.ttplatform.wh.data.FileConstant.SNAPSHOT_FILE_HEADER_SPACE_SIZE;

/**
 * @author Wang Hao
 * @date 2021/7/1 14:23
 */
public class SnapshotFileMetadataRegion{

    private static final int RELATIVE_SNAPSHOT_FILE_SIZE_FIELD_POSITION = 0;
    private static final int RELATIVE_LAST_INCLUDE_INDEX_FIELD_POSITION = 8;
    private static final int RELATIVE_LAST_INCLUDE_TERM_FIELD_POSITION = 12;
    private final MetadataRegion region;
    private long fileSize;
    private int lastIncludeIndex;
    private int lastIncludeTerm;

    public SnapshotFileMetadataRegion(File file) {
        this(file, SNAPSHOT_FILE_HEADER_SPACE_POSITION, SNAPSHOT_FILE_HEADER_SPACE_SIZE);
    }

    public SnapshotFileMetadataRegion(File file, long position, long regionSize) {
        this.region = new MetadataRegion(file, position, regionSize);
        this.fileSize = region.readLong(RELATIVE_SNAPSHOT_FILE_SIZE_FIELD_POSITION);
        this.lastIncludeIndex = region.readInt(RELATIVE_LAST_INCLUDE_INDEX_FIELD_POSITION);
        this.lastIncludeTerm = region.readInt(RELATIVE_LAST_INCLUDE_TERM_FIELD_POSITION);
    }

    public void clear() {
        recordFileSize(0L);
        recordLastIncludeIndex(0);
        recordLastIncludeTerm(0);
    }

    public void recordFileSize(long size) {
        if (size != fileSize) {
            region.writeLong(RELATIVE_SNAPSHOT_FILE_SIZE_FIELD_POSITION, size);
            this.fileSize = size;
        }
    }

    public long getFileSize() {
        return fileSize;
    }

    public void recordLastIncludeIndex(int lastIncludeIndex) {
        if (lastIncludeIndex != this.lastIncludeIndex) {
            region.writeInt(RELATIVE_LAST_INCLUDE_INDEX_FIELD_POSITION, lastIncludeIndex);
            this.lastIncludeIndex = lastIncludeIndex;
        }
    }

    public int getLastIncludeIndex() {
        return lastIncludeIndex;
    }

    public void recordLastIncludeTerm(int lastIncludeTerm) {
        if (lastIncludeTerm != this.lastIncludeTerm) {
            region.writeInt(RELATIVE_LAST_INCLUDE_TERM_FIELD_POSITION, lastIncludeTerm);
            this.lastIncludeTerm = lastIncludeTerm;
        }
    }

    public int getLastIncludeTerm() {
        return lastIncludeTerm;
    }

    public void force() {
        region.force();
    }
}
