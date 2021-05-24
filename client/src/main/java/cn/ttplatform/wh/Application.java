package cn.ttplatform.wh;

import cn.ttplatform.wh.config.ClientProperties;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * @author Wang Hao
 * @date 2021/5/21 18:53
 */
public class Application {

    public static void main(String[] args) throws ParseException {
        new Application().run(args);
    }

    private void run(String[] args) throws ParseException {
        CommandLine commandLine = parseOptions(args);
        if (commandLine == null) {
            return;
        }
        ClientProperties properties = initConfig(commandLine);
    }

    private CommandLine parseOptions(String[] args) throws ParseException {
        Options options = new Options();
        options.addOption(Option.builder("i")
            .hasArg()
            .argName("master-id")
            .desc("master node id.")
            .build());
        options.addOption(Option.builder("h")
            .hasArg()
            .argName("host")
            .desc("host of master node.")
            .build());
        options.addOption(Option.builder("p")
            .hasArg()
            .argName("port")
            .type(Number.class)
            .desc("port of master node.")
            .build());
        options.addOption(Option.builder("c")
            .hasArg()
            .argName("properties-path")
            .desc("properties file path.")
            .build());
        CommandLineParser parser = new DefaultParser();
        return parser.parse(options, args);
    }

    public ClientProperties initConfig(CommandLine commandLine) {
        ClientProperties properties;
        if (commandLine.hasOption('c')) {
            properties = new ClientProperties(commandLine.getOptionValue('c'));
        } else {
            properties = new ClientProperties();
        }
        if (commandLine.hasOption('i')) {
            properties.setMasterId(commandLine.getOptionValue('i'));
        }
        if (commandLine.hasOption('h')) {
            properties.setHost(commandLine.getOptionValue('h'));
        }
        if (commandLine.hasOption('p')) {
            properties.setPort(Integer.parseInt(commandLine.getOptionValue('p')));
        }
        return properties;
    }
}
