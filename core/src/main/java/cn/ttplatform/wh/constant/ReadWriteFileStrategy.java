package cn.ttplatform.wh.constant;

/**
 * @author Wang Hao
 * @date 2021/4/21 13:50
 */
public class ReadWriteFileStrategy {

    private ReadWriteFileStrategy() {
    }

    @Deprecated
    public static final String RANDOM = "random access";

    public static final String DIRECT = "direct access";
    public static final String INDIRECT = "indirect access";
}
