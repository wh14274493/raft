package cn.ttplatform.wh.common;

import java.net.InetSocketAddress;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * @author Wang Hao
 * @date 2021/4/21 13:30
 */
@Getter
@Setter
@ToString
@AllArgsConstructor
public class EndpointMetaData {

    private String nodeId;
    private String host;
    private int port;


    public InetSocketAddress getAddress() {
        return new InetSocketAddress(host, port);
    }
}
