package cn.ttplatform.wh.data.index;

import cn.ttplatform.wh.data.support.MetadataRegion;

import java.io.File;

import static cn.ttplatform.wh.data.FileConstant.LOG_INDEX_FILE_HEADER_SPACE_POSITION;
import static cn.ttplatform.wh.data.FileConstant.LOG_INDEX_FILE_HEADER_SPACE_SIZE;

/**
 * @author Wang Hao
 * @date 2021/7/1 14:20
 */
public class LogIndexFileMetadataRegion{

    private static final int RELATIVE_LOG_INDEX_FILE_SIZE_FIELD_POSITION = 0;
    private final MetadataRegion region;
    private long fileSize;

    public LogIndexFileMetadataRegion(File file) {
        this(file, LOG_INDEX_FILE_HEADER_SPACE_POSITION, LOG_INDEX_FILE_HEADER_SPACE_SIZE);
    }

    public LogIndexFileMetadataRegion(File file, long position, long regionSize) {
        this.region = new MetadataRegion(file, position, regionSize);
        this.fileSize = region.readLong(RELATIVE_LOG_INDEX_FILE_SIZE_FIELD_POSITION);
    }

    public void clear() {
        recordFileSize(0L);
    }

    public void recordFileSize(long size) {
        if (size != fileSize) {
            region.writeLong(RELATIVE_LOG_INDEX_FILE_SIZE_FIELD_POSITION, size);
            fileSize = size;
        }
    }

    public long getFileSize() {
        return fileSize;
    }

    public void force() {
        region.force();
    }
}
