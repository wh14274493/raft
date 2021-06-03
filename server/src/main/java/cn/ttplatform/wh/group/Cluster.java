package cn.ttplatform.wh.group;

import cn.ttplatform.wh.GlobalContext;
import cn.ttplatform.wh.config.ServerProperties;
import cn.ttplatform.wh.constant.ErrorMessage;
import cn.ttplatform.wh.data.LogManager;
import cn.ttplatform.wh.data.log.Log;
import cn.ttplatform.wh.message.SyncingMessage;
import cn.ttplatform.wh.support.Pool;
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
        Pool<LinkedBuffer> linkedBufferPool = context.getLinkedBufferPool();
        this.newConfigFactory = new NewConfigFactory(linkedBufferPool);
        this.oldNewConfigFactory = new OldNewConfigFactory(linkedBufferPool);
    }

    private Set<Endpoint> initClusterEndpoints(ServerProperties properties) {
        String clusterInfo = properties.getClusterInfo();
        if (clusterInfo == null || "".equals(clusterInfo)) {
            return Collections.emptySet();
        }
        return Arrays.stream(clusterInfo.split(" ")).map(Endpoint::new).collect(Collectors.toSet());
    }

    /**
     * When a node becomes the new leader, this method must be executed to reset the log replication status of the follower
     *
     * @param initLeftEdge  left edge
     * @param initRightEdge right edge
     */
    public void resetReplicationStates(int initLeftEdge, int initRightEdge) {
        endpointMap.forEach((id, endpoint) -> endpoint.resetReplicationState(initLeftEdge, initRightEdge));
        if (phase != Phase.STABLE) {
            newConfigMap.forEach((id, endpoint) -> endpoint.resetReplicationState(initLeftEdge, initRightEdge));
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

    /**
     * Calculate the new commitIndex based on the current phase of the cluster: 1. If the cluster is in the STABLE phase, only the
     * majority of nodes in oldConfig need to agree to submit the log. 2. If the cluster is in the NEW phase, only the majority of
     * nodes in newConfig need to agree to submit the log. 3. If the cluster is in the OLD_NEW phase, you need to agree to the
     * majority of nodes in newConfig and oldConfig before you can submit the log
     *
     * @return index needed to be committed
     */
    public int getNewCommitIndex() {
        if (phase == Phase.OLD_NEW) {
            int oldConfigCommitIndex =
                endpointMap.size() <= 1 ? context.getLogManager().getNextIndex() - 1 : getNewCommitIndexFrom(endpointMap);
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
        int oldConfigCommitIndex =
            endpointMap.size() <= 1 ? context.getLogManager().getNextIndex() - 1 : getNewCommitIndexFrom(endpointMap);
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
        // After sorting from small to large according to the matchIndex, take
        // the matchIndex of the left node of the middle node to be the new commitIndex
        Collections.sort(endpoints);
        return endpoints.get(size >> 1).getMatchIndex();
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
        SyncingMessage syncingMessage = new SyncingMessage();
        getAllEndpointExceptSelf().forEach(endpoint -> context.sendMessage(syncingMessage, endpoint));
    }

    /**
     * The SYNCING phase is added based on the original joint consensus. The task of this phase is to synchronize the logs of the
     * newly added nodes. Only after the synchronization is completed can the OLD_NEW phase be entered. Therefore, this phase can
     * be skipped directly if there is no new node. Only when all the newly added nodes have copied the expected set log (index =
     * logSynCompleteState) is the synchronization completed.
     *
     * @return Is it done
     */
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

    /**
     * All newly added nodes have synchronized logs to the specified state. Begin to enter the OLD_NEW phase
     */
    public void enterOldNewPhase() {
        if (phase != Phase.STABLE && phase != Phase.SYNCING) {
            log.warn("current phase[{}] is not STABLE or SYNCING.", phase);
            return;
        }
        if (context.getNode().isLeader()) {
            context.pendingLog(Log.OLD_NEW, getOldNewConfigBytes());
            log.info("pending OLD_NEW log");
        }
        phase = Phase.OLD_NEW;
        log.info("enter OLD_NEW phase");
    }

    /**
     * Leader and follower enter the NEW phase at different times. The follower enters the NEW phase after receiving the NEW log
     * from the leader, and the leader enters the NEW phase after submitting the OLD_NEW log. Moreover, after the leader enters
     * the NEW phase, if it finds that it is not in the newConfig, it will not exit the cluster directly, but needs to wait for
     * the NEW log to be submitted before exiting the cluster, but the follower will exit the cluster after receiving the NEW log
     * and replying to the leader.
     */
    public void enterNewPhase() {
        if (phase != Phase.OLD_NEW) {
            log.warn("current phase[{}] is not OLD_NEW.", phase);
            return;
        }
        phase = Phase.NEW;
        if (context.getNode().isLeader()) {
            log.info("pending NEW log");
            context.pendingLog(Log.NEW, getNewConfigBytes());
        }
        log.info("enter NEW phase.");
    }

    /**
     * Entering the STABLE stage indicates that the cluster change has been completed, and nodes not in newConfig will actively
     * become followers. And newConfig will replace oldConfig.
     */
    public void enterStablePhase() {
        if (phase != Phase.NEW) {
            log.warn("current phase[{}] is not NEW.", phase);
            return;
        }
        if (!inNewConfig(selfId)) {
            context.getNode().changeToFollower(context.getNode().getTerm(), null, null, 0, 0, 0L);
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
        newConfigMap.clear();
        LogManager logManager = context.getLogManager();
        metaData.forEach(endpointMetaData -> {
            Endpoint endpoint = endpointMap.get(endpointMetaData.getNodeId());
            if (endpoint == null) {
                endpoint = new Endpoint(endpointMetaData);
                endpoint.resetReplicationState(logManager.getLastIncludeIndex(), logManager.getNextIndex());
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
        OldNewConfig oldNewConfig = oldNewConfigFactory.create(config, config.length);
        updateNewConfigMap(oldNewConfig.getNewConfigs());
        log.debug("apply oldNew config, oldConfig is {}, newConfig is {}", endpointMap, newConfigMap);
    }

    public void applyNewConfig(byte[] config) {
        NewConfig newConfig = newConfigFactory.create(config, config.length);
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

