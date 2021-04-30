package cn.ttplatform.wh.core.group;

import static org.junit.jupiter.api.Assertions.*;

import cn.ttplatform.wh.config.ServerProperties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * @author Wang Hao
 * @date 2021/4/30 1:31
 */
class ClusterTest {

    @BeforeEach
    void setUp() {
        ServerProperties properties = new ServerProperties();
    }

    @AfterEach
    void tearDown() {
    }

    @Test
    void resetReplicationStates() {
    }

    @Test
    void find() {
    }

    @Test
    void countOfOldConfig() {
    }

    @Test
    void countOfNewConfig() {
    }

    @Test
    void getAllEndpointExceptSelf() {
    }

    @Test
    void getAllEndpointMetaData() {
    }

    @Test
    void getNewCommitIndex() {
    }

    @Test
    void synHasComplete() {
    }

    @Test
    void isSyncingNode() {
    }

    @Test
    void inNewConfig() {
    }

    @Test
    void inOldConfig() {
    }

    @Test
    void enterSyncingPhase() {
    }

    @Test
    void enterOldNewPhase() {
    }

    @Test
    void enterNewPhase() {
    }

    @Test
    void enterStablePhase() {
    }

    @Test
    void updateNewConfigMap() {
    }

    @Test
    void applyOldNewConfig() {
    }

    @Test
    void applyNewConfig() {
    }

    @Test
    void getOldNewConfigBytes() {
    }

    @Test
    void getNewConfigBytes() {
    }
}