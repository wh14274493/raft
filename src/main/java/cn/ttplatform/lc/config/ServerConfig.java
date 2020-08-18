package cn.ttplatform.lc.config;

import cn.ttplatform.lc.exception.OperateFileException;
import lombok.Data;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * @author Wang Hao
 * @date 2020/6/30 下午9:30
 */
@Data
public class ServerConfig {

    /**
     * Minimum election timeout
     */
    private int minElectionTimeout;
    /**
     * Maximum election timeout
     */
    private int maxElectionTimeout;
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
    private String nodeStateFilePath;
    private int snapshotGenerateThreshold;

    public ServerConfig(String configPath) {
        Properties properties = new Properties();
        try (FileInputStream fis = new FileInputStream(configPath)) {
            properties.load(fis);
            minElectionTimeout = Integer.parseInt(properties.getProperty("minElectionTimeout"));
            maxElectionTimeout = Integer.parseInt(properties.getProperty("maxElectionTimeout"));
            logReplicationDelay = Long.parseLong(properties.getProperty("logReplicationDelay"));
            logReplicationInterval = Long.parseLong(properties.getProperty("logReplicationInterval"));
            nodeStateFilePath = properties.getProperty("nodeStateFilePath");
            snapshotGenerateThreshold = Integer.parseInt(properties.getProperty("snapshotGenerateThreshold"));
        } catch (IOException e) {
            throw new OperateFileException(e.getMessage());
        }
    }
}
