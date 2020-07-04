package cn.ttplatform.lc.node;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * @author Wang Hao
 * @date 2020/6/30 下午9:46
 */
public class NodeGroup {

    private final String selfId;
    private final Map<String, Endpoint> nodeEndpointMap;

    public NodeGroup(Endpoint endpoint) {
        this(Collections.singleton(endpoint), endpoint.getNodeId());
    }

    public NodeGroup(Collection<Endpoint> endpoints, String selfId) {
        this.nodeEndpointMap = buildMap(endpoints);
        this.selfId = selfId;
    }

    private Map<String, Endpoint> buildMap(Collection<Endpoint> endpoints) {
        Map<String, Endpoint> endpointMap = new HashMap<>((int) (endpoints.size() / 0.75f + 1));
        endpoints.forEach(endpoint -> endpointMap.put(endpoint.getNodeId(), endpoint));
        return endpointMap;
    }

    public Endpoint find(String nodeId) {
        return nodeEndpointMap.get(nodeId);
    }

    /**
     * List all endpoint except self
     *
     * @return endpoint list
     */
    public List<Endpoint> listAllEndpointExceptSelf() {
        List<Endpoint> result = new ArrayList<>();
        nodeEndpointMap.forEach((s, endpoint) -> {
            if (!Objects.equals(selfId, s)) {
                result.add(endpoint);
            }
        });
        return result;
    }

    public void remove(String nodeId) {
        nodeEndpointMap.remove(nodeId);
    }
}
