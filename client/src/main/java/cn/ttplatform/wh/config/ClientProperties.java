package cn.ttplatform.wh.config;

import cn.ttplatform.wh.core.MemberInfo;
import cn.ttplatform.wh.exception.OperateFileException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Properties;
import lombok.Data;

/**
 * @author Wang Hao
 * @date 2021/4/20 13:03
 */
@Data
public class ClientProperties {

    private LinkedList<MemberInfo> memberInfos;
    private int linkedBuffPollSize;
    private int readIdleTimeout;
    private int writeIdleTimeout;
    private int allIdleTimeout;
    private int workerThreads;
    private String idGenerateStrategy;
    private int maxConnections;

    public ClientProperties(String configPath) {
        Properties properties = new Properties();
        File file = new File(configPath, "client.properties");
        try (FileInputStream fis = new FileInputStream(file)) {
            properties.load(fis);
            loadProperties(properties);
        } catch (IOException e) {
            throw new OperateFileException(e.getMessage());
        }
    }

    public ClientProperties() {
        Properties properties = new Properties();
        try (InputStream fis = this.getClass().getResourceAsStream("client.properties")) {
            properties.load(fis);
            loadProperties(properties);
        } catch (IOException e) {
            throw new OperateFileException(e.getMessage());
        }
    }

    private void loadProperties(Properties properties) {
        memberInfos = parseClusterInfo(properties.getProperty("cluster"));
        linkedBuffPollSize = Integer.parseInt(properties.getProperty("linkedBuffPollSize", "16"));
        readIdleTimeout = Integer.parseInt(properties.getProperty("readIdleTimeout", "10"));
        writeIdleTimeout = Integer.parseInt(properties.getProperty("writeIdleTimeout", "10"));
        allIdleTimeout = Integer.parseInt(properties.getProperty("allIdleTimeout", "10"));
        workerThreads = Integer.parseInt(properties.getProperty("workerThreads", "1"));
        idGenerateStrategy = properties
            .getProperty("idGenerateStrategy", "cn.ttplatform.wh.core.support.UUIDGenerator");
        maxConnections = Integer.parseInt(properties.getProperty("maxConnections", "10"));
    }

    private LinkedList<MemberInfo> parseClusterInfo(String clusterInfo) {
        String[] clusterConfig = clusterInfo.split(" ");
        LinkedList<MemberInfo> res = new LinkedList<>();
        Arrays.stream(clusterConfig).forEach(memberInfo -> {
            String[] pieces = memberInfo.split(",");
            if (pieces.length != 3) {
                throw new IllegalArgumentException("illegal node info [" + memberInfo + "]");
            }
            String nodeId = pieces[0];
            String host = pieces[1];
            int port;
            try {
                port = Integer.parseInt(pieces[2]);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("illegal port in node info [" + memberInfo + "]");
            }
            res.add(MemberInfo.builder().nodeId(nodeId).host(host).port(port).build());
        });
        return res;
    }
}
