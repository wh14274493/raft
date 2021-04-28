package cn.ttplatform.wh.core.group;

import java.net.InetSocketAddress;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;

/**
 * @author Wang Hao
 * @date 2021/4/21 13:30
 */
@Data
@ToString
@AllArgsConstructor
public class EndpointMetaData {

    private String nodeId;
    private String host;
    private int port;

    public EndpointMetaData(String metaData) {
        String[] pieces = metaData.split(",");
        if (pieces.length != 3) {
            throw new IllegalArgumentException("illegal node info [" + metaData + "]");
        }
        nodeId = pieces[0];
        host = pieces[1];
        try {
            port = Integer.parseInt(pieces[2]);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("illegal port in node info [" + metaData + "]");
        }
    }


    public InetSocketAddress getAddress() {
        return new InetSocketAddress(host, port);
    }
}
