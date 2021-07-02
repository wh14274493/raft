package cn.ttplatform.wh.data.support;

import java.io.File;
import java.nio.ByteBuffer;

import static cn.ttplatform.wh.data.FileConstant.LOG_FILE_HEADER_SPACE_POSITION;
import static cn.ttplatform.wh.data.FileConstant.LOG_FILE_HEADER_SPACE_SIZE;

/**
 * @author Wang Hao
 * @date 2021/7/1 14:12
 */
public class LogFileMetadataRegion implements FileHeaderOperator {

    private static final int RELATIVE_LOG_FILE_SIZE_FIELD_POSITION = 0;
    private final MetadataRegion region;

    public LogFileMetadataRegion(File file) {
        this(file, LOG_FILE_HEADER_SPACE_POSITION, LOG_FILE_HEADER_SPACE_SIZE);
    }

    public LogFileMetadataRegion(File file, long position, long regionSize) {
        this.region = new MetadataRegion(file, position, regionSize);
    }

    public ByteBuffer read() {
        return region.read();
    }

    public void write(ByteBuffer byteBuffer) {
        region.write(byteBuffer);
    }

    public void clear() {
        recordFileSize(0L);
    }

    @Override
    public void recordFileSize(long size) {
        region.writeLong(RELATIVE_LOG_FILE_SIZE_FIELD_POSITION, size);
    }

    @Override
    public long getFileSize() {
        return region.readLong(RELATIVE_LOG_FILE_SIZE_FIELD_POSITION);
    }

    @Override
    public void force() {
        region.force();
    }
}
