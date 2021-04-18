package cn.ttplatform.wh.config;

import cn.ttplatform.wh.exception.OperateFileException;
import io.netty.channel.EventLoopGroup;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
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
    private int listeningPort;

    /**
     * the number of thread used in server side {@link EventLoopGroup}
     */
    private int serverListenThreads;

    /**
     * the number of thread used in server side {@link EventLoopGroup}
     */
    private int serverWorkerThreads;

    /**
     * The service communicates with other nodes through this port number
     */
    private int communicationPort;

    /**
     * the number of thread used in client side {@link EventLoopGroup}
     */
    private int clientListenThreads;

    /**
     * the number of thread used in client side {@link EventLoopGroup}
     */
    private int clientWorkerThreads;

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

    private int buffPollSize;

    private int readIdleTimeout;

    private int writeIdleTimeout;

    private int allIdleTimeout;

    private String clusterInfo;

    public ServerProperties(String configPath) {
        if (configPath == null || "".equals(configPath)) {
            configPath = System.getProperty("user.home");
        }
        Properties properties = new Properties();
        File file = new File(configPath, "server.properties");
        try (FileInputStream fis = new FileInputStream(file)) {
            properties.load(fis);
            nodeId = properties.getProperty("nodeId", UUID.randomUUID().toString());
            listeningPort = Integer.parseInt(properties.getProperty("listeningPort", "8888"));
            serverListenThreads = Integer.parseInt(properties.getProperty("serverListenThreads", "1"));
            serverWorkerThreads = Integer.parseInt(properties.getProperty("serverWorkerThreads", "1"));
            communicationPort = Integer.parseInt(properties.getProperty("communicationPort", "8889"));
            clientListenThreads = Integer.parseInt(properties.getProperty("clientListenThreads", "1"));
            clientWorkerThreads = Integer.parseInt(properties.getProperty("clientWorkerThreads", "1"));
            minElectionTimeout = Integer.parseInt(properties.getProperty("minElectionTimeout", "3000"));
            maxElectionTimeout = Integer.parseInt(properties.getProperty("maxElectionTimeout", "4000"));
            logReplicationDelay = Long.parseLong(properties.getProperty("logReplicationDelay", "1000"));
            logReplicationInterval = Long.parseLong(properties.getProperty("logReplicationInterval", "1000"));
            replicationHeartBeat = Long.parseLong(properties.getProperty("replicationHeartBeat","800"));
            basePath = properties.getProperty("basePath", System.getProperty("user.home"));
            snapshotGenerateThreshold = Integer.parseInt(properties.getProperty("snapshotGenerateThreshold", String.valueOf(1024*1024*10)));
            maxTransferLogs = Integer.parseInt(properties.getProperty("maxTransferLogs", "50"));
            maxTransferSize = Integer.parseInt(properties.getProperty("maxTransferSize","10240"));
            buffPollSize = Integer.parseInt(properties.getProperty("buffPollSize", "16"));
            readIdleTimeout = Integer.parseInt(properties.getProperty("readIdleTimeout", "10"));
            writeIdleTimeout = Integer.parseInt(properties.getProperty("writeIdleTimeout", "10"));
            allIdleTimeout = Integer.parseInt(properties.getProperty("allIdleTimeout", "10"));
            clusterInfo = properties.getProperty("clusterInfo");
        } catch (IOException e) {
            throw new OperateFileException(e.getMessage());
        }
    }

}
