package cn.ttplatform.wh.core.group;

import cn.ttplatform.wh.config.ServerProperties;
import cn.ttplatform.wh.constant.ErrorMessage;
import cn.ttplatform.wh.core.GlobalContext;
import cn.ttplatform.wh.core.log.entry.LogEntry;
import cn.ttplatform.wh.exception.ClusterConfigException;
import cn.ttplatform.wh.support.BufferPool;
import io.protostuff.LinkedBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Wang Hao
 * @date 2020/6/30 下午9:46
 */
@Getter
@Slf4j
public class Cluster {

    private static final int MIN_CLUSTER_SIZE = 2;
    private Mode mode;
    private final String selfId;
    private final GlobalContext context;
    private final Map<String, Endpoint> newConfigMap = new HashMap<>();
    private final Map<String, Endpoint> endpointMap;
    private final NewConfigFactory newConfigFactory;
    private final OldNewConfigFactory oldNewConfigFactory;
    private Phase phase;
    private int logSynCompleteState;

    public Cluster(GlobalContext context) {
        this.context = context;
        Set<Endpoint> endpoints = initClusterEndpoints(context.getProperties());
        this.endpointMap = buildMap(endpoints);
        this.selfId = context.getProperties().getNodeId();
        this.phase = Phase.STABLE;
        BufferPool<LinkedBuffer> linkedBufferPool = context.getLinkedBufferPool();
        this.newConfigFactory = new NewConfigFactory(linkedBufferPool);
        this.oldNewConfigFactory = new OldNewConfigFactory(linkedBufferPool);
    }

    private Set<Endpoint> initClusterEndpoints(ServerProperties properties) {
        String clusterInfo = properties.getClusterInfo();
        if (clusterInfo == null || "".equals(clusterInfo)) {
            mode = Mode.SINGLE;
            return Collections.emptySet();
        }
        mode = Mode.CLUSTER;
        return Arrays.stream(clusterInfo.split(" ")).map(Endpoint::new).collect(Collectors.toSet());
    }

    public void resetReplicationStates(int nextIndex) {
        endpointMap.values().forEach(endpoint -> endpoint.resetReplicationState(nextIndex));
        if (phase != Phase.STABLE) {
            newConfigMap.forEach((id, endpoint) -> endpoint.resetReplicationState(nextIndex));
        }
    }

    private Map<String, Endpoint> buildMap(Collection<Endpoint> endpoints) {
        Map<String, Endpoint> map = new HashMap<>((int) (endpoints.size() / 0.75f + 1));
        endpoints.forEach(endpoint -> map.put(endpoint.getNodeId(), endpoint));
        return map;
    }

    public Endpoint find(String nodeId) {
        if (phase == Phase.STABLE) {
            return endpointMap.get(nodeId);
        }
        Endpoint endpoint = endpointMap.get(nodeId);
        return endpoint == null ? newConfigMap.get(nodeId) : endpoint;
    }

    public int countOfOldConfig() {
        return endpointMap.size();
    }

    public int countOfNewConfig() {
        return newConfigMap.size();
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
        if (phase != Phase.STABLE) {
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
            int oldConfigCommitIndex = getNewCommitIndexFrom(endpointMap);
            int newConfigCommitIndex = getNewCommitIndexFrom(newConfigMap);
            log.debug("oldConfigCommitIndex is {}.", oldConfigCommitIndex);
            log.debug("newConfigCommitIndex is {}.", newConfigCommitIndex);
            return Math.min(oldConfigCommitIndex, newConfigCommitIndex);
        }
        if (phase == Phase.NEW) {
            int newConfigCommitIndex = getNewCommitIndexFrom(newConfigMap);
            log.debug("newConfigCommitIndex is {}.", newConfigCommitIndex);
            return newConfigCommitIndex;
        }
        int oldConfigCommitIndex = getNewCommitIndexFrom(endpointMap);
        log.debug("oldConfigCommitIndex is {}.", oldConfigCommitIndex);
        return oldConfigCommitIndex;
    }

    private int getNewCommitIndexFrom(Map<String, Endpoint> endpointMap) {
        List<Endpoint> endpoints = new ArrayList<>(endpointMap.size());
        endpointMap.forEach((id, endpoint) -> {
            if (!selfId.equals(id)) {
                endpoints.add(endpoint);
            }
        });
        int size = endpoints.size();
        if (size < MIN_CLUSTER_SIZE) {
            throw new ClusterConfigException(ErrorMessage.CLUSTER_SIZE_ERROR);
        }
        Collections.sort(endpoints);
        return endpoints.get(size >> 1).getMatchIndex();
    }

    public boolean synHasComplete() {
        if (phase != Phase.SYNCING) {
            throw new UnsupportedOperationException(String.format(ErrorMessage.NOT_SYNCING_PHASE, phase));
        }
        for (Entry<String, Endpoint> entry : newConfigMap.entrySet()) {
            if (!selfId.equals(entry.getKey()) && entry.getValue().getMatchIndex() < logSynCompleteState) {
                log.info("log syn is uncompleted, entry is {}, logSynCompleteState is {}.", entry, logSynCompleteState);
                return false;
            }
        }
        log.info("log syn is completed");
        return true;
    }

    public boolean isSyncingNode(String nodeId) {
        if (phase != Phase.SYNCING) {
            throw new UnsupportedOperationException(String.format(ErrorMessage.NOT_SYNCING_PHASE, phase));
        }
        boolean res = newConfigMap.containsKey(nodeId) && !endpointMap.containsKey(nodeId);
        log.info("{} is syncing node, return {}", nodeId, res);
        return res;
    }

    public boolean inNewConfig(String nodeId) {
        return newConfigMap.containsKey(nodeId);
    }

    public boolean inOldConfig(String nodeId) {
        return endpointMap.containsKey(nodeId);
    }

    public void enterSyncingPhase() {
        if (phase != Phase.STABLE) {
            log.warn("current phase[{}] is not STABLE.", phase);
            return;
        }
        logSynCompleteState = getNewCommitIndex();
        log.info("logSynCompleteState is {}", logSynCompleteState);
        phase = Phase.SYNCING;
        log.info("enter SYNCING phase");
    }

    /**
     * All newly added nodes have synchronized logs to the specified state. Begin to enter the Coldnew phase
     */
    public void enterOldNewPhase() {
        if (phase != Phase.STABLE && phase != Phase.SYNCING) {
            log.warn("current phase[{}] is not STABLE or SYNCING.", phase);
            return;
        }
        if (context.isLeader()) {
            context.pendingLog(LogEntry.OLD_NEW, getOldNewConfigBytes());
            log.info("pending OLD_NEW log");
        }
        phase = Phase.OLD_NEW;
        log.info("enter OLD_NEW phase");
    }

    public void enterNewPhase() {
        if (phase != Phase.OLD_NEW) {
            log.warn("current phase[{}] is not OLD_NEW.", phase);
            return;
        }
        phase = Phase.NEW;
        if (context.isLeader()) {
            log.info("pending NEW log");
            context.pendingLog(LogEntry.NEW, getNewConfigBytes());
        }
        log.info("enter NEW phase.");
    }

    public void enterStablePhase() {
        if (phase != Phase.NEW) {
            log.warn("current phase[{}] is not NEW.", phase);
            return;
        }
        if (!inNewConfig(selfId)) {
            context.changeToFollower(context.getNode().getTerm(), null, null, 0, 0, 0L);
        }
        endpointMap.clear();
        if (inNewConfig(selfId)) {
            endpointMap.putAll(newConfigMap);
        }
        newConfigMap.clear();
        phase = Phase.STABLE;
        log.info("enter STABLE phase. current cluster config is {}", endpointMap);
    }

    public boolean updateNewConfigMap(Set<EndpointMetaData> metaData) {
        AtomicInteger count = new AtomicInteger();
        int nextIndex = context.getLog().getNextIndex();
        newConfigMap.clear();
        metaData.forEach(endpointMetaData -> {
            Endpoint endpoint = endpointMap.get(endpointMetaData.getNodeId());
            if (endpoint == null) {
                endpoint = new Endpoint(endpointMetaData);
                endpoint.resetReplicationState(nextIndex);
                count.getAndIncrement();
            } else {
                endpoint.setMetaData(endpointMetaData);
            }
            newConfigMap.put(endpointMetaData.getNodeId(), endpoint);
        });
        log.debug("updateNewConfigMap {}", metaData);
        return count.get() == 0;
    }

    public void applyOldNewConfig(byte[] config) {
        OldNewConfig oldNewConfig = oldNewConfigFactory.create(config);
        updateNewConfigMap(oldNewConfig.getNewConfigs());
        log.debug("apply oldNew config, oldConfig is {}, newConfig is {}", endpointMap, newConfigMap);
    }

    public void applyNewConfig(byte[] config) {
        NewConfig newConfig = newConfigFactory.create(config);
        newConfigMap.clear();
        newConfig.getNewConfigs().forEach(
            endpointMetaData -> newConfigMap.put(endpointMetaData.getNodeId(), new Endpoint(endpointMetaData)));
        log.debug("apply new config, newConfigMap is {}", newConfigMap);
    }

    public byte[] getOldNewConfigBytes() {
        Set<EndpointMetaData> oldConfigs = new HashSet<>(endpointMap.size());
        endpointMap.forEach((id, endpoint) -> oldConfigs.add(endpoint.getMetaData()));
        Set<EndpointMetaData> newConfigs = new HashSet<>(newConfigMap.size());
        newConfigMap.forEach((id, endpoint) -> newConfigs.add(endpoint.getMetaData()));
        OldNewConfig oldNewConfig = new OldNewConfig(oldConfigs, newConfigs);
        return oldNewConfigFactory.getBytes(oldNewConfig);
    }

    public byte[] getNewConfigBytes() {
        List<EndpointMetaData> newConfigs = new ArrayList<>(newConfigMap.size());
        newConfigMap.forEach((id, endpoint) -> newConfigs.add(endpoint.getMetaData()));
        NewConfig newConfig = new NewConfig(newConfigs);
        return newConfigFactory.getBytes(newConfig);
    }

}

