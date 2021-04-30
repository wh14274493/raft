package cn.ttplatform.wh;

import cn.ttplatform.wh.config.ServerProperties;
import cn.ttplatform.wh.core.Node;
import cn.ttplatform.wh.core.GlobalContext;
import lombok.extern.slf4j.Slf4j;
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
@Slf4j
public class Application {

    private void run(String[] args) throws ParseException {
        CommandLine commandLine = parseOptions(args);
        if (commandLine == null) {
            return;
        }
        ServerProperties properties = initConfig(commandLine);
        GlobalContext context = new GlobalContext(properties);
        Node node = new Node(context);
        context.register(node);
        node.start();
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
            properties.setPort(Integer.parseInt(commandLine.getOptionValue('p')));
        }
        if (commandLine.hasOption("d")) {
            properties.setBasePath(commandLine.getOptionValue('d'));
        }
        return properties;
    }

    public static void main(String[] args) throws Exception {
        new Application().run(args);
    }
}
