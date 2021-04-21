package cn.ttplatform.wh;

import cn.ttplatform.wh.config.ServerProperties;
import cn.ttplatform.wh.constant.ReadWriteFileStrategy;
import cn.ttplatform.wh.core.Cluster;
import cn.ttplatform.wh.core.ClusterMember;
import cn.ttplatform.wh.core.MemberInfo;
import cn.ttplatform.wh.core.Node;
import cn.ttplatform.wh.core.NodeContext;
import cn.ttplatform.wh.core.NodeState;
import cn.ttplatform.wh.core.StateMachine;
import cn.ttplatform.wh.core.connector.nio.NioConnector;
import cn.ttplatform.wh.core.log.FileLog;
import cn.ttplatform.wh.core.support.BufferPool;
import cn.ttplatform.wh.core.support.DefaultScheduler;
import cn.ttplatform.wh.core.support.DirectByteBufferPool;
import cn.ttplatform.wh.core.support.IndirectByteBufferPool;
import cn.ttplatform.wh.core.support.SingleThreadTaskExecutor;
import cn.ttplatform.wh.server.nio.NioListener;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * @author Wang Hao
 * @date 2021/3/15 15:25
 */
public class Application {

    private void run(String[] args) throws ParseException {
        CommandLine commandLine = parseOptions(args);
        if (commandLine == null) {
            return;
        }
        ServerProperties properties = initConfig(commandLine);
        NodeContext nodeContext = buildContext(properties);

        StateMachine stateMachine = new StateMachine();
        Node node = Node.builder()
            .selfId(properties.getNodeId())
            .context(nodeContext)
            .connector(new NioConnector(nodeContext))
            .listener(new NioListener(nodeContext))
            .stateMachine(stateMachine)
            .build();
        stateMachine.register(node);
        nodeContext.register(node);
        node.start();
    }

    private NodeContext buildContext(ServerProperties properties) {
        Set<ClusterMember> members = initClusterMembers(properties);
        File base = new File(properties.getBasePath());
        BufferPool<ByteBuffer> pool;
        if (ReadWriteFileStrategy.DIRECT.equals(properties.getReadWriteFileStrategy())) {
            pool = new DirectByteBufferPool(properties.getByteBufferPoolSize(), properties.getByteBufferSizeLimit());
        } else {
            pool = new IndirectByteBufferPool(properties.getByteBufferPoolSize(), properties.getByteBufferSizeLimit());
        }
        return NodeContext.builder()
            .cluster(new Cluster(members, properties.getNodeId()))
            .scheduler(new DefaultScheduler(properties))
            .taskExecutor(new SingleThreadTaskExecutor())
            .nodeState(new NodeState(base, pool))
            .log(new FileLog(base, pool))
            .properties(properties)
            .build();
    }

    private CommandLine parseOptions(String[] args) throws ParseException {
        Options options = new Options();
        options.addOption(Option.builder("i")
            .longOpt("id")
            .hasArg()
            .argName("node-id")
            .desc("node id, required. must be unique in group. ")
            .build());
        options.addOption(Option.builder("h")
            .hasArg()
            .argName("host")
            .desc("host of node, required.")
            .build());
        options.addOption(Option.builder("p")
            .longOpt("port-server")
            .hasArg()
            .argName("port")
            .type(Number.class)
            .desc("port of server, required.")
            .build());
        options.addOption(Option.builder("P")
            .longOpt("port-connector")
            .hasArg()
            .argName("port")
            .type(Number.class)
            .desc("port of connector, required")
            .build());
        options.addOption(Option.builder("d")
            .hasArg()
            .argName("data-dir")
            .desc("data directory, optional. must be present")
            .build());
        options.addOption(Option.builder("c")
            .hasArg()
            .argName("properties-path")
            .desc("properties file path.")
            .build());
        options.addOption(Option.builder("C")
            .hasArgs()
            .argName("node-endpoint")
            .desc(
                "cluster config, required. format: <node-endpoint> <node-endpoint>..., format of node-endpoint: <node-id>,<host>,<port-raft-node>, eg: A,localhost,8000 B,localhost,8010")
            .build());
        if (args.length == 0) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("raft-db [OPTION]...", options);
            return null;
        }
        CommandLineParser parser = new DefaultParser();
        return parser.parse(options, args);
    }

    public ServerProperties initConfig(CommandLine commandLine) {
        ServerProperties properties;
        if (commandLine.hasOption("c")) {
            properties = new ServerProperties(commandLine.getOptionValue('c'));
        } else {
            properties = new ServerProperties();
        }
        if (commandLine.hasOption("C")) {
            properties.setClusterInfo(commandLine.getOptionValue("C"));
        }
        if (commandLine.hasOption("i")) {
            properties.setNodeId(commandLine.getOptionValue('i'));
        }
        if (commandLine.hasOption("p")) {
            properties.setListeningPort(Integer.parseInt(commandLine.getOptionValue('p')));
        }
        if (commandLine.hasOption("P")) {
            properties.setCommunicationPort(Integer.parseInt(commandLine.getOptionValue('p')));
        }
        if (commandLine.hasOption("d")) {
            properties.setBasePath(commandLine.getOptionValue('d'));
        }
        return properties;
    }

    private Set<ClusterMember> initClusterMembers(ServerProperties properties) {
        String[] clusterConfig = properties.getClusterInfo().split(" ");
        return Arrays.stream(clusterConfig).map(memberInfo -> {
            String[] pieces = memberInfo.split(",");
            if (pieces.length != 3) {
                throw new IllegalArgumentException("illegal node info [" + memberInfo + "]");
            }
            String nodeId = pieces[0];
            String host = pieces[1];
            int port;
            try {
                port = Integer.parseInt(pieces[2]);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("illegal port in node info [" + memberInfo + "]");
            }
            return ClusterMember.builder().memberInfo(MemberInfo.builder().nodeId(nodeId).host(host).port(port).build())
                .build();
        }).collect(Collectors.toSet());
    }

    public static void main(String[] args) throws Exception {
        new Application().run(args);
    }
}
