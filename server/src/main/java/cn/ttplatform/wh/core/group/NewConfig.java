package cn.ttplatform.wh.core.group;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Wang Hao
 * @date 2021/4/28 10:29
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
class NewConfig {

    private List<EndpointMetaData> newConfigs;
}
