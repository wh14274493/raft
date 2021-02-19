package cn.ttplatform.lc.core.store.log;

/**
 * @author Wang Hao
 * @date 2021/2/14 16:22
 */
public interface Generation {

    /**
     * Node state will be stored in {@code nodeStoreFile}
     */
    String NODE_STATE_FILE_NAME = "node.state";

    /**
     * snapshot data will be stored in {@code snapshotFileName}
     */
    String SNAPSHOT_FILE_NAME = "log.snapshot";

    /**
     * log entry data will be stored in {@code logEntryFileName}
     */
    String LOG_ENTRY_FILE_NAME = "log.data";

    /**
     * index data will be stored in {@code indexFileName}
     */
    String INDEX_FILE_NAME = "log.index";

    /**
     * directory name for young generation
     */
    String INSTALLING_FILE_NAME = "installing";

    /**
     * close the opened file in this directory
     */
    void close();
}
