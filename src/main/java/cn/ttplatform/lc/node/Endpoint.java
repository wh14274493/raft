package cn.ttplatform.lc.node;

import java.net.InetSocketAddress;
import java.util.Objects;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * @author Wang Hao
 * @date 2020/6/30 下午9:42
 */
@Data
@Builder
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class Endpoint {

    private String nodeId;
    private InetSocketAddress address;

    @Override
    public int hashCode() {
        return Objects.hash(this.nodeId);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof Endpoint)) {
            return false;
        }
        return Objects.equals(this.nodeId, ((Endpoint) object).nodeId);
    }
}
