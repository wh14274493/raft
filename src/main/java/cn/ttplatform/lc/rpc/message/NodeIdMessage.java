package cn.ttplatform.lc.rpc.message;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

/**
 * @author : wang hao
 * @description : NodeIdMessage
 * @date :  2020/8/16 12:39
 **/
@Getter
@Setter
@Builder
@AllArgsConstructor
public class NodeIdMessage implements Message {

    private String nodeId;
}
