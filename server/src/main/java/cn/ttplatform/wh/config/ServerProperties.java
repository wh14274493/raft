package cn.ttplatform.wh.config;

import cn.ttplatform.wh.exception.OperateFileException;
import io.netty.channel.EventLoopGroup;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.UUID;
import lombok.Data;

/**
 * @author Wang Hao
 * @date 2020/6/30 下午9:30
 */
@Data
public class ServerProperties {

    /**
     * an unique id
     */
    private String nodeId;

    /**
     * The service will listen for connections from this port
     */
    private int port;

    /**
     * the number of thread used in server side {@link EventLoopGroup}
     */
    private int bossThreads;

    /**
     * the number of thread used in server side {@link EventLoopGroup}
     */
    private int workerThreads;

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
     * The task will be executed every {@code logReplicationInterval} milliseconds6
     */
    private long logReplicationInterval;

    /**
     * replicationHeartBeat should less than {@code logReplicationInterval} milliseconds6
     */
    private long replicationHeartBeat;

    /**
     * all the data will be stored in {@code basePath}
     */
    private String basePath;

    /**
     * Each {@code snapshotGenerateThreshold} logs added generates a snapshot of the logs
     */
    private int snapshotGenerateThreshold;

    /**
     * The maximum number of transmission logs
     */
    private int maxTransferLogs;

    /**
     * The maximum number of transmission byte size
     */
    private int maxTransferSize;

    /**
     * {@code linkedBuffPollSize} used for serialize/deserialize obj
     */
    private int linkedBuffPollSize;

    private int readIdleTimeout;

    private int writeIdleTimeout;

    private int allIdleTimeout;

    private String clusterInfo;

    /**
     * random access / direct access / indirect access
     */
    private String readWriteFileStrategy;

    /**
     * only used when readWriteFileStrategy is direct access / indirect access. see {@link
     * cn.ttplatform.wh.constant.ReadWriteFileStrategy}
     */
    private int byteBufferPoolSize;

    /**
     * only used when readWriteFileStrategy is direct access / indirect access
     */
    private int byteBufferSizeLimit;

    public ServerProperties(String configPath) {
        Properties properties = new Properties();
        File file = new File(configPath, "server.properties");
        try (FileInputStream fis = new FileInputStream(file)) {
            properties.load(fis);
            loadProperties(properties);
        } catch (IOException e) {
            throw new OperateFileException(e.getMessage());
        }
    }

    public ServerProperties() {
        Properties properties = new Properties();
        try (InputStream fis = this.getClass().getResourceAsStream("server.properties")) {
            properties.load(fis);
            loadProperties(properties);
        } catch (IOException e) {
            throw new OperateFileException(e.getMessage());
        }
    }

    private void loadProperties(Properties properties) {
        nodeId = properties.getProperty("nodeId", UUID.randomUUID().toString());
        port = Integer.parseInt(properties.getProperty("port", "8888"));
        bossThreads = Integer.parseInt(properties.getProperty("bossThreads", "1"));
        workerThreads = Integer.parseInt(properties.getProperty("workerThreads", "1"));
        minElectionTimeout = Integer.parseInt(properties.getProperty("minElectionTimeout", "3000"));
        maxElectionTimeout = Integer.parseInt(properties.getProperty("maxElectionTimeout", "4000"));
        logReplicationDelay = Long.parseLong(properties.getProperty("logReplicationDelay", "1000"));
        logReplicationInterval = Long.parseLong(properties.getProperty("logReplicationInterval", "1000"));
        replicationHeartBeat = Long.parseLong(properties.getProperty("replicationHeartBeat", "800"));
        basePath = properties.getProperty("basePath", System.getProperty("user.home"));
        snapshotGenerateThreshold = Integer
            .parseInt(properties.getProperty("snapshotGenerateThreshold", String.valueOf(1024 * 1024 * 10)));
        maxTransferLogs = Integer.parseInt(properties.getProperty("maxTransferLogs", "500"));
        maxTransferSize = Integer.parseInt(properties.getProperty("maxTransferSize", "10240"));
        linkedBuffPollSize = Integer.parseInt(properties.getProperty("linkedBuffPollSize", "16"));
        readIdleTimeout = Integer.parseInt(properties.getProperty("readIdleTimeout", "10"));
        writeIdleTimeout = Integer.parseInt(properties.getProperty("writeIdleTimeout", "10"));
        allIdleTimeout = Integer.parseInt(properties.getProperty("allIdleTimeout", "10"));
        clusterInfo = properties.getProperty("clusterInfo");
        readWriteFileStrategy = properties.getProperty("readWriteFileStrategy", "direct access");
        byteBufferPoolSize = Integer.parseInt(properties.getProperty("byteBufferPoolSize", "10"));
        byteBufferSizeLimit = Integer
            .parseInt(properties.getProperty("byteBufferSizeLimit", String.valueOf(1024 * 1024 * 10)));
    }

}
