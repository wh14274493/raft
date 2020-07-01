package cn.ttplatform.lc.environment;

import lombok.Data;

/**
 * @author Wang Hao
 * @date 2020/6/30 下午9:30
 */
@Data
public class SettingProperties {

    private int minElectionTimeout;
    private int maxElectionTimeout;
    private int logReplicationDelay;
    private int logReplicationInterval;
}
