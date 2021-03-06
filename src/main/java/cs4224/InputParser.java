package cs4224;

import org.apache.commons.cli.*;

// Reference: https://lightrun.com/java/java-tutorial-java-command-line-arguments/
public class InputParser {
    private Options options = new Options();
    private DefaultParser parser = new DefaultParser();
    private HelpFormatter helpFormatter = new HelpFormatter();

    public InputParser() {
        addOption("t", "task", "Type of task: transaction or dbstate", true, true);
        addOption("k", "keyspace", "Keyspace name", true, true);
        addOption("i", "ip", "IP address of cassandra cluster", true, false);
        addOption("p", "port", "Port of cassandra cluster", true, false);
        addOption("f", "fileName", "Name of query file", true, false);
        addOption("l", "logFileName", "Name of log file", true, false);
    }

    private void addOption(String opt, String longOpt, String description, boolean hasArg, boolean isRequired) {
        Option option = new Option(opt, longOpt, hasArg, description);
        option.setRequired(isRequired);
        options.addOption(option);
    }

    public CommandLine parse(String[] args) {
        try {
            final CommandLine cli = parser.parse(options, args);
            if (cli.getOptionValue("t").equalsIgnoreCase("transaction") && !cli.hasOption("f")) {
                throw new MissingArgumentException("Missing argument for option: fileName");
            }
            return cli;
        } catch (ParseException e) {
            System.err.println(e.getMessage());
            helpFormatter.printHelp("Main", options);
            return null;
        }
    }
}
