package cn.ttplatform.wh.core;

import cn.ttplatform.wh.common.EndpointMetaData;
import cn.ttplatform.wh.constant.ExceptionMessage;
import cn.ttplatform.wh.exception.ClusterConfigException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Wang Hao
 * @date 2020/6/30 下午9:46
 */
public class Cluster {

    private static final int MIN_CLUSTER_SIZE = 3;
    private final String selfId;
    private final Map<String, Endpoint> endpointMap;
    private int activeSize;

    public Cluster(Endpoint endpoint) {
        this(Collections.singleton(endpoint), endpoint.getNodeId());
    }

    public Cluster(Collection<Endpoint> endpoints, String selfId) {
        this.endpointMap = buildMap(endpoints);
        this.activeSize = endpointMap.size();
        this.selfId = selfId;
    }

    public void resetReplicationStates(int nextIndex) {
        endpointMap.values().forEach(endpoint -> endpoint.resetReplicationState(nextIndex));
    }

    private Map<String, Endpoint> buildMap(Collection<Endpoint> endpoints) {
        Map<String, Endpoint> map = new HashMap<>((int) (endpoints.size() / 0.75f + 1));
        endpoints.forEach(endpoint -> map.put(endpoint.getNodeId(), endpoint));
        return map;
    }

    public Endpoint find(String nodeId) {
        return endpointMap.get(nodeId);
    }

    public int countAll() {
        return endpointMap.size();
    }

    public int countOfActive() {
        return activeSize;
    }

    /**
     * List all endpoint except self
     *
     * @return endpoint list
     */
    public List<Endpoint> getAllEndpointExceptSelf() {
        List<Endpoint> result = new ArrayList<>();
        endpointMap.forEach((id, endpoint) -> {
            if (!selfId.equals(id)) {
                result.add(endpoint);
            }
        });
        return result;
    }

    public List<EndpointMetaData> getAllEndpointMetaData() {
        List<EndpointMetaData> result = new ArrayList<>();
        endpointMap.forEach((id, endpoint) -> result.add(endpoint.getEndpointMetaData()));
        return result;
    }

    public void remove(String nodeId) {
        endpointMap.remove(nodeId);
        activeSize--;
        if (activeSize < MIN_CLUSTER_SIZE) {
            throw new ClusterConfigException(ExceptionMessage.CLUSTER_SIZE_ERROR);
        }
    }

    public int getNewCommitIndex() {
        List<Endpoint> endpoints = getAllEndpointExceptSelf();
        int size = endpoints.size();
        if (size < MIN_CLUSTER_SIZE - 1) {
            throw new ClusterConfigException(ExceptionMessage.CLUSTER_SIZE_ERROR);
        }
        Collections.sort(endpoints);
        return endpoints.get(size >> 1).getMatchIndex();
    }
}
