package cn.ttplatform.wh.data.support;

import java.io.File;

import static cn.ttplatform.wh.data.FileConstant.*;

/**
 * @author Wang Hao
 * @date 2021/7/1 14:23
 */
public class SnapshotFileMetadataRegion implements FileHeaderOperator {

    private static final int RELATIVE_SNAPSHOT_FILE_SIZE_FIELD_POSITION = 0;
    private static final int RELATIVE_LAST_INCLUDE_INDEX_FIELD_POSITION = 8;
    private static final int RELATIVE_LAST_INCLUDE_TERM_FIELD_POSITION = 12;
    private final MetadataRegion region;

    public SnapshotFileMetadataRegion(File file) {
        this(file, SNAPSHOT_FILE_HEADER_SPACE_POSITION, SNAPSHOT_FILE_HEADER_SPACE_SIZE);
    }

    public SnapshotFileMetadataRegion(File file, long position, long regionSize) {
        this.region = new MetadataRegion(file, position, regionSize);
    }

    public void recordLastIncludeIndex(int lastIncludeIndex) {
        region.writeInt(RELATIVE_LAST_INCLUDE_INDEX_FIELD_POSITION, lastIncludeIndex);
    }

    public int getLastIncludeIndex() {
        return region.readInt(RELATIVE_LAST_INCLUDE_INDEX_FIELD_POSITION);
    }

    public void recordLastIncludeTerm(int lastIncludeTerm) {
        region.writeInt(RELATIVE_LAST_INCLUDE_TERM_FIELD_POSITION, lastIncludeTerm);
    }

    public int getLastIncludeTerm() {
        return region.readInt(RELATIVE_LAST_INCLUDE_TERM_FIELD_POSITION);
    }

    public void clear() {
        recordFileSize(0L);
        recordLastIncludeIndex(0);
        recordLastIncludeTerm(0);
    }

    @Override
    public void recordFileSize(long size) {
        region.writeLong(RELATIVE_SNAPSHOT_FILE_SIZE_FIELD_POSITION, size);
    }

    @Override
    public long getFileSize() {
        return region.readLong(RELATIVE_SNAPSHOT_FILE_SIZE_FIELD_POSITION);
    }

    @Override
    public void force() {
        region.force();
    }
}
