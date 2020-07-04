package cn.ttplatform.lc.environment;

import lombok.Data;

/**
 * @author Wang Hao
 * @date 2020/6/30 下午9:30
 */
@Data
public class RaftProperties {

    /**
     * Minimum election timeout
     */
    private long minElectionTimeout;
    /**
     * Maximum election timeout
     */
    private long maxElectionTimeout;
    /**
     * Replication log task will start in {@code logReplicationDelay} milliseconds
     */
    private long logReplicationDelay;
    /**
     * The task will be executed every {@code logReplicationInterval} milliseconds
     */
    private long logReplicationInterval;
    /**
     * Node state will be stored in {@code nodeStoreFile}
     */
    private String nodeStoreFile;
}
