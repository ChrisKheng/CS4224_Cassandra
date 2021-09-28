package cs4224;

import org.apache.commons.cli.*;

// Reference: https://lightrun.com/java/java-tutorial-java-command-line-arguments/
public class InputParser {
    private Options options = new Options();
    private DefaultParser parser = new DefaultParser();
    private HelpFormatter helpFormatter = new HelpFormatter();

    public InputParser() {
        Option fileName = new Option("f", "filename", true, "Name of query file");
        fileName.setRequired(true);
        options.addOption(fileName);

        Option keyspace = new Option("k", "keyspace", true, "Keyspace name");
        keyspace.setRequired(true);
        options.addOption(keyspace);

        Option ip = new Option("i", "ip", true, "IP address " +
                "of cassandra cluster");
        options.addOption(ip);

        Option port = new Option("p", "port", true, "Port of cassandra cluster");
        options.addOption(port);
    }

    public CommandLine parse(String[] args) {
        try {
            return parser.parse(options, args);
        } catch (ParseException e) {
            System.err.println(e.getMessage());
            helpFormatter.printHelp("Main", options);
            return null;
        }
    }
}
