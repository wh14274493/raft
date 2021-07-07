package cn.ttplatform.wh.data.log;

import cn.ttplatform.wh.data.support.MetadataRegion;
import lombok.extern.slf4j.Slf4j;

import java.io.File;

import static cn.ttplatform.wh.data.FileConstant.LOG_FILE_HEADER_SPACE_POSITION;
import static cn.ttplatform.wh.data.FileConstant.LOG_FILE_HEADER_SPACE_SIZE;

/**
 * @author Wang Hao
 * @date 2021/7/1 14:12
 */
@Slf4j
public class LogFileMetadataRegion{

    private static final int RELATIVE_LOG_FILE_SIZE_FIELD_POSITION = 0;
    private final MetadataRegion region;
    private long fileSize;

    public LogFileMetadataRegion(File file) {
        this(file, LOG_FILE_HEADER_SPACE_POSITION, LOG_FILE_HEADER_SPACE_SIZE);
    }

    public LogFileMetadataRegion(File file, long position, long regionSize) {
        this.region = new MetadataRegion(file, position, regionSize);
        this.fileSize = region.readLong(RELATIVE_LOG_FILE_SIZE_FIELD_POSITION);
    }

    public void clear() {
        recordFileSize(0L);
    }

    public void recordFileSize(long size) {
        if (size != fileSize) {
            region.writeLong(RELATIVE_LOG_FILE_SIZE_FIELD_POSITION, size);
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
