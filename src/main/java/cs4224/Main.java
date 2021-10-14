package cs4224;
import com.datastax.oss.driver.api.core.CqlSession;
import com.google.inject.Guice;
import com.google.inject.Injector;
import cs4224.module.BaseModule;
import org.apache.commons.cli.CommandLine;
import org.apache.logging.log4j.LogManager;

import java.util.concurrent.ExecutorService;

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

        String task = parsedArguments.getOptionValue("t");
        String keyspace = parsedArguments.getOptionValue("k");
        String ip = parsedArguments.hasOption("i") ? parsedArguments.getOptionValue("i") : "";
        int port = parsedArguments.hasOption("p") ? Integer.parseInt(parsedArguments.getOptionValue("p")) : -1;
        Injector injector = Guice.createInjector(new BaseModule(keyspace, ip, port));

        switch (task.toLowerCase()) {
            case "transaction":
                String logFileName = parsedArguments.hasOption("l") ? parsedArguments.getOptionValue("l") : "";
                setLogFileName(logFileName);
                String fileName = parsedArguments.getOptionValue("f");
                final Driver driver = injector.getInstance(Driver.class);
                driver.runQueries(fileName);
                break;
            case "dbstate":
                final DBState dbState = injector.getInstance(DBState.class);
                dbState.save();
                break;
            default:
                // throw new Exception("Unknown transaction types");
                System.err.println("Unknown task type");
        }

        final CqlSession cqlSession = injector.getInstance(CqlSession.class);
        final ExecutorService executorService = injector.getInstance(ExecutorService.class);
        cqlSession.close();
        executorService.shutdown();
    }

    private static void setLogFileName(String name) {
        String fileName = name;
        if (name.isEmpty()) {
            fileName = "out.log";
        }

        // Reference: // https://newbedev.com/log4j2-assigning-file-appender-filename-at-runtime
        System.setProperty("logFilename", fileName);
        org.apache.logging.log4j.core.LoggerContext ctx =
                (org.apache.logging.log4j.core.LoggerContext) LogManager.getContext(false);
        ctx.reconfigure();
    }
}
