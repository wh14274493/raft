package cn.ttplatform.wh.data;

import cn.ttplatform.wh.data.log.LogFileMetadataRegion;
import cn.ttplatform.wh.data.index.LogIndexFileMetadataRegion;
import cn.ttplatform.wh.data.snapshot.SnapshotFileMetadataRegion;

import java.io.File;
import java.util.Arrays;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * @author Wang Hao
 * @date 2021/3/16 0:52
 */
public class FileConstant {

    private FileConstant() {
    }


    public static final Pattern SNAPSHOT_NAME_PATTERN = Pattern.compile("(\\d+)_(\\d+).snapshot");
    public static final Pattern LOG_NAME_PATTERN = Pattern.compile("(\\d+)_(\\d+).log");
    public static final Pattern INDEX_NAME_PATTERN = Pattern.compile("(\\d+)_(\\d+).index");

    public static final String SNAPSHOT_NAME_SUFFIX = ".snapshot";
    public static final String LOG_NAME_SUFFIX = ".log";
    public static final String INDEX_NAME_SUFFIX = ".index";

    public static final String SNAPSHOT_GENERATING_FILE_NAME = "%d_%d.snapshot";
    public static final String LOG_GENERATING_FILE_NAME = "%d_%d.log";
    public static final String INDEX_GENERATING_FILE_NAME = "%d_%d.index";

    public static final String EMPTY_SNAPSHOT_FILE_NAME = "0_0.snapshot";
    public static final String EMPTY_LOG_FILE_NAME = "0_0.log";
    public static final String EMPTY_INDEX_FILE_NAME = "0_0.index";

    public static final String METADATA_FILE_NAME = "node.metadata";

    public static final long NODE_STATE_SPACE_POSITION = 0;
    public static final int NODE_STATE_SPACE_SIZE = 128;

    public static final long LOG_FILE_HEADER_SPACE_POSITION = NODE_STATE_SPACE_POSITION + NODE_STATE_SPACE_SIZE;
    public static final int LOG_FILE_HEADER_SPACE_SIZE = 8;

    public static final long GENERATING_LOG_FILE_HEADER_SPACE_POSITION = LOG_FILE_HEADER_SPACE_POSITION + LOG_FILE_HEADER_SPACE_SIZE;
    public static final int GENERATING_LOG_FILE_HEADER_SPACE_SIZE = LOG_FILE_HEADER_SPACE_SIZE;

    public static final long LOG_INDEX_FILE_HEADER_SPACE_POSITION = GENERATING_LOG_FILE_HEADER_SPACE_POSITION + GENERATING_LOG_FILE_HEADER_SPACE_SIZE;
    public static final int LOG_INDEX_FILE_HEADER_SPACE_SIZE = 8;

    public static final long GENERATING_LOG_INDEX_FILE_HEADER_SPACE_POSITION = LOG_INDEX_FILE_HEADER_SPACE_POSITION + LOG_INDEX_FILE_HEADER_SPACE_SIZE;
    public static final int GENERATING_LOG_INDEX_FILE_HEADER_SPACE_SIZE = LOG_INDEX_FILE_HEADER_SPACE_SIZE;

    public static final long SNAPSHOT_FILE_HEADER_SPACE_POSITION = GENERATING_LOG_INDEX_FILE_HEADER_SPACE_POSITION + GENERATING_LOG_INDEX_FILE_HEADER_SPACE_SIZE;
    public static final int SNAPSHOT_FILE_HEADER_SPACE_SIZE = 16;

    public static final long GENERATING_SNAPSHOT_FILE_HEADER_SPACE_POSITION = SNAPSHOT_FILE_HEADER_SPACE_POSITION + SNAPSHOT_FILE_HEADER_SPACE_SIZE;
    public static final int GENERATING_SNAPSHOT_FILE_HEADER_SPACE_SIZE = SNAPSHOT_FILE_HEADER_SPACE_SIZE;

    public static File newSnapshotFile(File parent, int lastIncludeIndex, int lastIncludeTerm) {
        return new File(parent, String.format(SNAPSHOT_GENERATING_FILE_NAME, lastIncludeTerm, lastIncludeIndex));
    }

    public static File newLogFile(File parent, int lastIncludeIndex, int lastIncludeTerm) {
        return new File(parent, String.format(LOG_GENERATING_FILE_NAME, lastIncludeTerm, lastIncludeIndex));
    }

    public static File newLogIndexFile(File parent, int lastIncludeIndex, int lastIncludeTerm) {
        return new File(parent, String.format(INDEX_GENERATING_FILE_NAME, lastIncludeTerm, lastIncludeIndex));
    }

    public static File getLatestSnapshotFile(File parent) {
        File[] files = parent.listFiles();
        if (files == null || files.length == 0) {
            return new File(parent, EMPTY_SNAPSHOT_FILE_NAME);
        }
        Optional<File> fileOptional = Arrays.stream(files)
                .filter(file -> SNAPSHOT_NAME_PATTERN.matcher(file.getName()).matches()).min((o1, o2) -> {
                    String o1Name = o1.getName();
                    String[] o1Pieces = o1Name.substring(0, o1Name.lastIndexOf('.')).split("_");
                    String o2Name = o2.getName();
                    String[] o2Pieces = o2Name.substring(0, o2Name.lastIndexOf('.')).split("_");
                    return Integer.parseInt(o2Pieces[1]) - Integer.parseInt(o1Pieces[1]);
                });
        return fileOptional.orElse(new File(parent, EMPTY_SNAPSHOT_FILE_NAME));
    }

    public static File getMatchedLogFile(String path) {
        return new File(path.replace(SNAPSHOT_NAME_SUFFIX, LOG_NAME_SUFFIX));
    }

    public static File getMatchedLogIndexFile(String path) {
        return new File(path.replace(SNAPSHOT_NAME_SUFFIX, INDEX_NAME_SUFFIX));
    }

    public static LogFileMetadataRegion getLogFileMetadataRegion(File file) {
        return new LogFileMetadataRegion(file);
    }

    public static LogFileMetadataRegion getGeneratingLogFileMetadataRegion(File file) {
        LogFileMetadataRegion region = new LogFileMetadataRegion(file, GENERATING_LOG_FILE_HEADER_SPACE_SIZE, GENERATING_LOG_FILE_HEADER_SPACE_SIZE);
        region.clear();
        return region;
    }

    public static LogIndexFileMetadataRegion getLogIndexFileMetadataRegion(File file) {
        return new LogIndexFileMetadataRegion(file);
    }

    public static LogIndexFileMetadataRegion getGeneratingLogIndexFileMetadataRegion(File file) {
        LogIndexFileMetadataRegion region = new LogIndexFileMetadataRegion(file, GENERATING_LOG_INDEX_FILE_HEADER_SPACE_POSITION, GENERATING_LOG_FILE_HEADER_SPACE_SIZE);
        region.clear();
        return region;
    }

    public static SnapshotFileMetadataRegion getSnapshotFileMetadataRegion(File file) {
        return new SnapshotFileMetadataRegion(file);
    }

    public static SnapshotFileMetadataRegion getGeneratingSnapshotFileMetadataRegion(File file) {
        SnapshotFileMetadataRegion region = new SnapshotFileMetadataRegion(file, GENERATING_SNAPSHOT_FILE_HEADER_SPACE_POSITION, GENERATING_SNAPSHOT_FILE_HEADER_SPACE_SIZE);
        region.clear();
        return region;
    }
}
