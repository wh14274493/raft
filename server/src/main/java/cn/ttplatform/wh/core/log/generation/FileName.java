package cn.ttplatform.wh.core.log.generation;

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
    public static final String EMPTY_FILE_NAME = "log-0-0";

    /**
     * directory name prefix for generation dir
     */
    public static final String GENERATION_FILE_NAME_PREFIX = "log-";
}
