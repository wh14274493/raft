package cn.ttplatform.wh.core.data;

/**
 * @author Wang Hao
 * @date 2021/3/16 0:52
 */
public class FileName {

    private FileName() {
    }


    /**
     * snapshot data will be stored in {@code snapshotFileName}
     */
    public static final String SNAPSHOT_FILE_NAME = "log.snapshot";

    /**
     * log entry data will be stored in {@code logEntryFileName}
     */
    public static final String LOG_ENTRY_FILE_NAME = "log.data";

    /**
     * index data will be stored in {@code indexFileName}
     */
    public static final String INDEX_FILE_NAME = "log.index";

    public static final String SNAPSHOT_GENERATING_FILE_NAME = "log-%d-%d.snapshot";

    public static final String LOG_GENERATING_FILE_NAME = "log-%d-%d.data";

    public static final String INDEX_GENERATING_FILE_NAME = "log-%d-%d.index";

    /**
     * directory name for installing generation
     */
    public static final String INSTALLING_FILE_NAME = "installing";

    /**
     * directory name for generating generation
     */
    public static final String GENERATING_FILE_NAME = "generating";

    /**
     * directory name for empty old generation
     */
    public static final String EMPTY_SNAPSHOT_FILE_NAME = "log-0-0.snapshot";
    public static final String EMPTY_LOG_FILE_NAME = "log-0-0.data";
    public static final String EMPTY_INDEX_FILE_NAME = "log-0-0.index";

    /**
     * directory name prefix for generation dir
     */
    public static final String GENERATION_FILE_NAME_PREFIX = "log-";
}
