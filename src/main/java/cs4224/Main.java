package cs4224;
import com.google.inject.Guice;
import com.google.inject.Injector;
import cs4224.module.BaseModule;
import org.apache.commons.cli.CommandLine;

public class Main {

    public static void main(String[] args) {
        System.out.println("[START OF PROGRAM]");
        try {
            run(args);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
        System.out.println("[END OF PROGRAM]");
    }

    private static void run(String[] args) throws Exception {
        InputParser parser = new InputParser();
        CommandLine parsedArguments = parser.parse(args);

        if (parsedArguments == null) {
            throw new IllegalArgumentException("Incorrect arguments");
        }

        String keyspace = parsedArguments.getOptionValue("k");
        String ip = parsedArguments.hasOption("i") ? parsedArguments.getOptionValue("i") : "";
        int port = parsedArguments.hasOption("p") ? Integer.parseInt(parsedArguments.getOptionValue("p")) : -1;

        String fileName = parsedArguments.getOptionValue("f");


        Injector injector = Guice.createInjector(new BaseModule(keyspace, ip, port));
        final Driver driver = injector.getInstance(Driver.class);

        driver.runQueries(fileName);
    }




}
