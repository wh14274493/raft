package cn.ttplatform.wh;

import static cn.ttplatform.wh.constant.LaunchOption.CLUSTER_OPTION;
import static cn.ttplatform.wh.constant.LaunchOption.HOST_OPTION;
import static cn.ttplatform.wh.constant.LaunchOption.ID_OPTION;
import static cn.ttplatform.wh.constant.LaunchOption.MODE_OPTION;
import static cn.ttplatform.wh.constant.LaunchOption.PORT_OPTION;

import cn.ttplatform.wh.config.RunMode;
import cn.ttplatform.wh.config.ServerProperties;
import cn.ttplatform.wh.constant.LaunchOption;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * @author Wang Hao
 * @date 2021/3/15 15:25
 */
public class ServerLauncher {

    private void run(String[] args) throws ParseException {
        CommandLine commandLine = parseOptions(args);
        if (commandLine == null) {
            return;
        }
        ServerProperties properties = initConfig(commandLine);
        Node node = new Node(properties);
        node.start();
        Runtime.getRuntime().addShutdownHook(new Thread(node::stop, "recycle"));
    }

    private CommandLine parseOptions(String[] args) throws ParseException {
        Options options = new Options();
        options.addOption(LaunchOption.ID);
        options.addOption(LaunchOption.HOST);
        options.addOption(LaunchOption.PORT);
        options.addOption(LaunchOption.CONFIG);
        options.addOption(LaunchOption.MODE);
        options.addOption(LaunchOption.CLUSTER);
        if (args.length == 0) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("[OPTION]...", options);
            return null;
        }
        CommandLineParser parser = new DefaultParser();
        return parser.parse(options, args);
    }

    public ServerProperties initConfig(CommandLine commandLine) {
        ServerProperties properties;
        if (commandLine.hasOption(LaunchOption.CONFIG_OPTION)) {
            String value = commandLine.getOptionValue('c');
            properties = new ServerProperties(value);
        } else {
            properties = new ServerProperties();
        }
        if (commandLine.hasOption(MODE_OPTION)) {
            String mode = commandLine.getOptionValue(MODE_OPTION);
            if (RunMode.SINGLE.toString().equals(mode.toUpperCase())) {
                properties.setMode(RunMode.SINGLE);
                properties.setClusterInfo(null);
            } else {
                properties.setMode(RunMode.CLUSTER);
                if (commandLine.hasOption(CLUSTER_OPTION)) {
                    properties.setClusterInfo(commandLine.getOptionValue(CLUSTER_OPTION));
                }
            }
        }
        if (commandLine.hasOption(ID_OPTION)) {
            properties.setNodeId(commandLine.getOptionValue(ID_OPTION));
        }
        if (commandLine.hasOption(HOST_OPTION)) {
            properties.setHost(commandLine.getOptionValue(HOST_OPTION));
        }
        if (commandLine.hasOption(PORT_OPTION)) {
            properties.setPort(Integer.parseInt(commandLine.getOptionValue(PORT_OPTION)));
        }
        return properties;
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("log4j.defaultInitOverride", "true");
        new ServerLauncher().run(args);
    }

}
