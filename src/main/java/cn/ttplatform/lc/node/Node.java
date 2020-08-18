package cn.ttplatform.lc.node;

import cn.ttplatform.lc.node.role.Role;

/**
 * @author : wang hao
 * @description : Node
 * @date :  2020/8/15 23:16
 **/
public class Node {

    private String selfId;
    private Role role;
    private NodeContext context;

    public void election() {
        context.cluster().listAllEndpointExceptSelf().forEach(member -> {

        });
    }


}
