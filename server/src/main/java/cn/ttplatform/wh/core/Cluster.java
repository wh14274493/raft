package cn.ttplatform.wh.core;

import cn.ttplatform.wh.cmd.ClusterChangeCommand;
import cn.ttplatform.wh.common.EndpointMetaData;
import cn.ttplatform.wh.config.ServerProperties;
import cn.ttplatform.wh.constant.ErrorMessage;
import cn.ttplatform.wh.exception.ClusterConfigException;
import cn.ttplatform.wh.support.BufferPool;
import cn.ttplatform.wh.support.Factory;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Wang Hao
 * @date 2020/6/30 下午9:46
 */
public class Cluster {

    public enum Phase {
        // Syncing logs to the newly added node
        SYNCING,
        // Log synchronization has been completed, and Coldnew logs have been added to the cluster.
        // At this stage, all logs need to pass the agreement of the majority of the new configuration
        // and the majority of the old configuration at the same time before they can be submitted.
        OLD_NEW,
        // Coldnew logs have been committed, and Cnew logs have been added to the cluster. At this stage,
        // all logs can be submitted only with the approval of the majority of the new configuration
        NEW,
        // Cnew logs have been committed, the cluster is stable. At this time, nodes that are no longer
        // in the new configuration will automatically go offline
        STABLE

    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    private static class OldNewConfig {

        private List<EndpointMetaData> newConfigs;
        private List<EndpointMetaData> oldConfigs;
    }

    private static class OldNewConfigFactory implements Factory<OldNewConfig> {

        private final BufferPool<LinkedBuffer> pool;
        private final Schema<OldNewConfig> schema = RuntimeSchema.getSchema(OldNewConfig.class);

        public OldNewConfigFactory(BufferPool<LinkedBuffer> pool) {
            this.pool = pool;
        }

        @Override
        public OldNewConfig create(byte[] content) {
            OldNewConfig oldNewConfig = new OldNewConfig();
            ProtostuffIOUtil.mergeFrom(content, oldNewConfig, schema);
            return oldNewConfig;
        }

        @Override
        public byte[] getBytes(OldNewConfig oldNewConfig) {
            return ProtostuffIOUtil.toByteArray(oldNewConfig, schema, pool.allocate());
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    private static class NewConfig {

        private List<EndpointMetaData> newConfigs;
    }

    private static class NewConfigFactory implements Factory<NewConfig> {

        private final BufferPool<LinkedBuffer> pool;
        private final Schema<NewConfig> schema = RuntimeSchema.getSchema(NewConfig.class);

        public NewConfigFactory(BufferPool<LinkedBuffer> pool) {
            this.pool = pool;
        }

        @Override
        public NewConfig create(byte[] content) {
            NewConfig newConfig = new NewConfig();
            ProtostuffIOUtil.mergeFrom(content, newConfig, schema);
            return newConfig;
        }

        @Override
        public byte[] getBytes(NewConfig newConfig) {
            return ProtostuffIOUtil.toByteArray(newConfig, schema, pool.allocate());
        }
    }

    private static final int MIN_CLUSTER_SIZE = 3;
    private final String selfId;
    private final NodeContext context;
    private final Map<String, Endpoint> newConfigMap = new HashMap<>();
    private final Map<String, Endpoint> endpointMap;
    private final NewConfigFactory newConfigFactory;
    private final OldNewConfigFactory oldNewConfigFactory;
    private Phase phase;
    private int logSynCompleteState;

    public Cluster(NodeContext context) {
        this.context = context;
        Set<Endpoint> endpoints = initClusterEndpoints(context.getProperties());
        this.endpointMap = buildMap(endpoints);
        this.selfId = context.getProperties().getNodeId();
        this.phase = Phase.STABLE;
        this.newConfigFactory = new NewConfigFactory(context.getLinkedBufferPool());
        this.oldNewConfigFactory = new OldNewConfigFactory(context.getLinkedBufferPool());
    }

    private Set<Endpoint> initClusterEndpoints(ServerProperties properties) {
        String[] clusterConfig = properties.getClusterInfo().split(" ");
        return Arrays.stream(clusterConfig).map(endpointMetaData -> {
            String[] pieces = endpointMetaData.split(",");
            if (pieces.length != 3) {
                throw new IllegalArgumentException("illegal node info [" + endpointMetaData + "]");
            }
            String nodeId = pieces[0];
            String host = pieces[1];
            int port;
            try {
                port = Integer.parseInt(pieces[2]);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("illegal port in node info [" + endpointMetaData + "]");
            }
            return new Endpoint(new EndpointMetaData(nodeId, host, port));
        }).collect(Collectors.toSet());
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

    public int countOfCluster() {
        return endpointMap.size();
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
        if (phase == Phase.SYNCING || phase == Phase.OLD_NEW) {
            newConfigMap.forEach((id, endpoint) -> {
                if (!selfId.equals(id) && !endpointMap.containsKey(id)) {
                    result.add(endpoint);
                }
            });
        }
        return result;
    }

    public List<EndpointMetaData> getAllEndpointMetaData() {
        List<EndpointMetaData> result = new ArrayList<>();
        endpointMap.forEach((id, endpoint) -> result.add(endpoint.getMetaData()));
        return result;
    }

    public int getNewCommitIndex() {
        if (phase == Phase.OLD_NEW) {
            return Math.min(getNewCommitIndexFrom(endpointMap), getNewCommitIndexFrom(newConfigMap));
        }
        if (phase == Phase.NEW) {
            return getNewCommitIndexFrom(newConfigMap);
        }
        return getNewCommitIndexFrom(endpointMap);
    }

    private int getNewCommitIndexFrom(Map<String, Endpoint> endpointMap) {
        List<Endpoint> endpoints = new ArrayList<>(endpointMap.size() - 1);
        newConfigMap.forEach((id, endpoint) -> {
            if (!selfId.equals(id)) {
                endpoints.add(endpoint);
            }
        });
        int size = endpoints.size();
        if (size < MIN_CLUSTER_SIZE - 1) {
            throw new ClusterConfigException(ErrorMessage.CLUSTER_SIZE_ERROR);
        }
        Collections.sort(endpoints);
        return endpoints.get(size >> 1).getMatchIndex();
    }

    public boolean synHasComplete() {
        for (Entry<String, Endpoint> entry : newConfigMap.entrySet()) {
            if (entry.getValue().getMatchIndex() < logSynCompleteState) {
                return false;
            }
        }
        return true;
    }

    public boolean isSyncingNode(String nodeId) {
        return newConfigMap.containsKey(nodeId) && !endpointMap.containsKey(nodeId);
    }

    public boolean inNewConfig(String nodeId) {
        return newConfigMap.containsKey(nodeId);
    }

    public void enterSyncingPhase(ClusterChangeCommand command) {
        phase = Phase.SYNCING;
        newConfigMap.clear();
        if (updateNewConfigMap(command.getNewConfig())) {
            // If there is no added node, go directly to the OLD_NEW stage
            context.pendingOldNewConfigLog();
            return;
        }
        logSynCompleteState = getNewCommitIndex();
    }

    /**
     * All newly added nodes have synchronized logs to the specified state. Begin to enter the Coldnew stage
     */
    public void enterOldNewPhase() {
        phase = Phase.OLD_NEW;
    }

    public void enterNewPhase() {
        phase = Phase.NEW;
    }

    public void enterStablePhase() {
        phase = Phase.STABLE;
        endpointMap.clear();
        endpointMap.putAll(newConfigMap);
        newConfigMap.clear();
    }

    public boolean updateNewConfigMap(List<EndpointMetaData> metaData) {
        AtomicInteger count = new AtomicInteger();
        metaData.forEach(endpointMetaData -> {
            Endpoint endpoint = endpointMap.get(endpointMetaData.getNodeId());
            if (endpoint == null) {
                endpoint = new Endpoint(endpointMetaData);
                count.getAndIncrement();
            } else {
                endpoint.setMetaData(endpointMetaData);
            }
            newConfigMap.put(endpointMetaData.getNodeId(), endpoint);
        });
        return count.get() == 0;
    }

    public void applyOldNewConfig(byte[] config) {
        OldNewConfig oldNewConfig = oldNewConfigFactory.create(config);
        updateNewConfigMap(oldNewConfig.getNewConfigs());
    }

    public void applyNewConfig(byte[] config) {
        NewConfig newConfig = newConfigFactory.create(config);
        newConfigMap.clear();
        newConfig.getNewConfigs().forEach(
            endpointMetaData -> newConfigMap.put(endpointMetaData.getNodeId(), new Endpoint(endpointMetaData)));
    }

    public byte[] getOldNewConfig() {
        List<EndpointMetaData> oldConfig = new ArrayList<>(endpointMap.size());
        endpointMap.forEach((id, endpoint) -> oldConfig.add(endpoint.getMetaData()));
        List<EndpointMetaData> newConfig = new ArrayList<>(newConfigMap.size());
        newConfigMap.forEach((id, endpoint) -> newConfig.add(endpoint.getMetaData()));
        OldNewConfig oldNewConfig = new OldNewConfig(oldConfig, newConfig);
        return oldNewConfigFactory.getBytes(oldNewConfig);
    }

    public byte[] getNewConfig() {
        List<EndpointMetaData> newConfigs = new ArrayList<>(newConfigMap.size());
        newConfigMap.forEach((id, endpoint) -> newConfigs.add(endpoint.getMetaData()));
        NewConfig newConfig = new NewConfig(newConfigs);
        return newConfigFactory.getBytes(newConfig);
    }

    public Phase getPhase() {
        return phase;
    }

    public void setPhase(Phase phase) {
        this.phase = phase;
    }
}
