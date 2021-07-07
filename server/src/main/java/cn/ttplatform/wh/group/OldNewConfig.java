package cn.ttplatform.wh.group;

import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Wang Hao
 * @date 2021/4/28 10:27
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
class OldNewConfig {

    private Set<EndpointMetaData> oldConfigs;
    private Set<EndpointMetaData> newConfigs;
}
