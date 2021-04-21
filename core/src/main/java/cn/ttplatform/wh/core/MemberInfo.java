package cn.ttplatform.wh.core;

import java.net.InetSocketAddress;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.ToString;

/**
 * @author Wang Hao
 * @date 2021/4/20 10:25
 */
@Data
@Builder
@ToString
@AllArgsConstructor
public class MemberInfo {

    private String nodeId;
    private String host;
    private int port;

    public InetSocketAddress getAddress() {
        return new InetSocketAddress(host, port);
    }

}
