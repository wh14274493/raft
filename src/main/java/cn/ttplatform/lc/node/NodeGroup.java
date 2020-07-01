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
    private final Map<String, NodeEndpoint> nodeEndpointMap;

    public NodeGroup(NodeEndpoint endpoint) {
        this(Collections.singleton(endpoint), endpoint.getNodeId());
    }

    public NodeGroup(Collection<NodeEndpoint> endpoints, String selfId) {
        this.nodeEndpointMap = buildMap(endpoints);
        this.selfId = selfId;
    }

    private Map<String, NodeEndpoint> buildMap(Collection<NodeEndpoint> endpoints) {
        Map<String, NodeEndpoint> endpointMap = new HashMap<>((int) (endpoints.size() / 0.75f + 1));
        endpoints.forEach(endpoint -> endpointMap.put(endpoint.getNodeId(), endpoint));
        return endpointMap;
    }

    public NodeEndpoint find(String nodeId) {
        return nodeEndpointMap.get(nodeId);
    }

    /**
     * list all endpoint except self
     *
     * @return endpoint list
     */
    public List<NodeEndpoint> listAll() {
        List<NodeEndpoint> result = new ArrayList<>();
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
