package cn.ttplatform.wh.config;

import cn.ttplatform.wh.exception.OperateFileException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import lombok.Getter;
import lombok.Setter;

/**
 * @author Wang Hao
 * @date 2021/5/21 18:46
 */
@Getter
@Setter
public class ClientProperties {

    private String masterId;
    private String host;
    private int port;

    public ClientProperties(String path) {
        Properties properties = new Properties();
        File file = new File(path, "server.properties");
        try (FileInputStream fis = new FileInputStream(file)) {
            properties.load(fis);
            loadProperties(properties);
        } catch (IOException e) {
            throw new OperateFileException(e.getMessage());
        }
    }

    public ClientProperties(){
        Properties properties = new Properties();
        try (InputStream fis = this.getClass().getResourceAsStream("server.properties")) {
            properties.load(fis);
            loadProperties(properties);
        } catch (IOException e) {
            throw new OperateFileException(e.getMessage());
        }
    }

    private void loadProperties(Properties properties) {
        masterId = properties.getProperty("nodeId");
        host = properties.getProperty("host", "127.0.0.1");
        port = Integer.parseInt(properties.getProperty("port", "8888"));
    }
}
