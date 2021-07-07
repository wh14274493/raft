package cn.ttplatform.wh.constant;

import org.apache.commons.cli.Option;

/**
 * @author Wang Hao
 * @date 2021/5/26 21:25
 */
public class LaunchOption {

    private LaunchOption() {
    }

    public static final String ID_OPTION = "i";
    public static final String ID_LONG_OPTION = "id";
    public static final String ID_ARG_NAME = "server-id";
    public static final String ID_DESC = "id of server, must be unique.";
    public static final Option ID = Option.builder(ID_OPTION)
        .longOpt(ID_LONG_OPTION)
        .argName(ID_ARG_NAME)
        .desc(ID_DESC)
        .hasArg()
        .build();
    public static final String HOST_OPTION = "h";
    public static final String HOST_LONG_OPTION = "host";
    public static final String HOST_ARG_NAME = "server-host";
    public static final String HOST_DESC = "host of server.";
    public static final Option HOST = Option.builder(HOST_OPTION)
        .longOpt(HOST_LONG_OPTION)
        .argName(HOST_ARG_NAME)
        .desc(HOST_DESC)
        .hasArg()
        .build();
    public static final String PORT_OPTION = "p";
    public static final String PORT_LONG_OPTION = "port";
    public static final String PORT_ARG_NAME = "server-port";
    public static final String PORT_DESC = "port of server.";
    public static final Option PORT = Option.builder(PORT_OPTION)
        .longOpt(PORT_LONG_OPTION)
        .argName(PORT_ARG_NAME)
        .desc(PORT_DESC)
        .hasArg()
        .build();
    public static final String CONFIG_OPTION = "c";
    public static final String CONFIG_LONG_OPTION = "config";
    public static final String CONFIG_ARG_NAME = "server-config";
    public static final String CONFIG_DESC = "config file path.";
    public static final Option CONFIG = Option.builder(CONFIG_OPTION)
        .longOpt(CONFIG_LONG_OPTION)
        .argName(CONFIG_ARG_NAME)
        .desc(CONFIG_DESC)
        .hasArg()
        .build();
    public static final String MODE_OPTION = "m";
    public static final String MODE_LONG_OPTION = "mode";
    public static final String MODE_ARG_NAME = "server-mode";
    public static final String MODE_DESC = "run mode.";
    public static final Option MODE = Option.builder(MODE_OPTION)
        .longOpt(MODE_LONG_OPTION)
        .argName(MODE_ARG_NAME)
        .desc(MODE_DESC)
        .hasArg()
        .build();
    public static final String CLUSTER_OPTION = "C";
    public static final String CLUSTER_LONG_OPTION = "cluster";
    public static final String CLUSTER_ARG_NAME = "cluster";
    public static final String CLUSTER_DESC =
        "cluster info. required in CLUSTER mode. format: <node-endpoint> <node-endpoint>..., format of node-endpoint: <node-id>,<host>,<port-raft-node>, eg: A,localhost,8000 B,localhost,8010";
    public static final Option CLUSTER = Option.builder(CLUSTER_OPTION)
        .longOpt(CLUSTER_LONG_OPTION)
        .argName(CLUSTER_ARG_NAME)
        .desc(CLUSTER_DESC)
        .hasArg()
        .build();

}
