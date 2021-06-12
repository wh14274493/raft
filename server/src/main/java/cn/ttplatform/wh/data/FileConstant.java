package cn.ttplatform.wh.data;

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

    public static final int LOG_FILE_HEADER_SIZE = 8;
    public static final int LOG_INDEX_FILE_HEADER_SIZE = 8;

    public static final Pattern SNAPSHOT_NAME_PATTERN = Pattern.compile("data-(\\d+)-(\\d+).snapshot");
    public static final Pattern LOG_NAME_PATTERN = Pattern.compile("data-(\\d+)-(\\d+).log");
    public static final Pattern INDEX_NAME_PATTERN = Pattern.compile("data-(\\d+)-(\\d+).index");

    public static final String SNAPSHOT_NAME_SUFFIX = ".snapshot";
    public static final String LOG_NAME_SUFFIX = ".log";
    public static final String INDEX_NAME_SUFFIX = ".index";

    public static final String SNAPSHOT_GENERATING_FILE_NAME = "data-%d-%d.snapshot";

    public static final String LOG_GENERATING_FILE_NAME = "data-%d-%d.log";

    public static final String INDEX_GENERATING_FILE_NAME = "data-%d-%d.index";

    /**
     * directory name for empty old generation
     */
    public static final String EMPTY_SNAPSHOT_FILE_NAME = "data-0-0.snapshot";
    public static final String EMPTY_LOG_FILE_NAME = "data-0-0.log";
    public static final String EMPTY_INDEX_FILE_NAME = "data-0-0.index";

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
                String[] o1Pieces = o1Name.substring(0, o1Name.lastIndexOf('.')).split("-");
                String o2Name = o2.getName();
                String[] o2Pieces = o2Name.substring(0, o2Name.lastIndexOf('.')).split("-");
                return Integer.parseInt(o2Pieces[2]) - Integer.parseInt(o1Pieces[2]);
            });
        return fileOptional.orElse(new File(parent, EMPTY_SNAPSHOT_FILE_NAME));
    }

    public static File getMatchedLogFile(String path) {
        return new File(path.replace(SNAPSHOT_NAME_SUFFIX, LOG_NAME_SUFFIX));
    }

    public static File getMatchedLogIndexFile(String path) {
        return new File(path.replace(SNAPSHOT_NAME_SUFFIX, INDEX_NAME_SUFFIX));
    }
}
