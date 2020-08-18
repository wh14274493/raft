package cn.ttplatform.lc.rpc.message;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

/**
 * @author : wang hao
 * @description : InstallSnapshotMessage
 * @date :  2020/8/16 13:17
 **/
@Getter
@Setter
@Builder
@AllArgsConstructor
public class InstallSnapshotMessage implements Message {

    private int term;
    private int lastIncludeIndex;
    private int lastIncludeTerm;
    private long offset;
    private byte[] chunk;
    private boolean done;

}
