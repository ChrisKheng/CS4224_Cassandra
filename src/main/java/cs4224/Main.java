package cs4224;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;

import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.session.SessionBuilder;

import cs4224.transactions.BaseTransaction;
import cs4224.transactions.DeliveryTransaction;
import cs4224.transactions.NewOrderTransaction;
import cs4224.transactions.OrderStatusTransaction;
import cs4224.transactions.PaymentTransaction;
import cs4224.transactions.PopularItemTransaction;
import cs4224.transactions.RelatedCustomerTransaction;
import cs4224.transactions.StockLevelTransaction;
import cs4224.transactions.TopBalanceTransaction;
import org.apache.commons.cli.CommandLine;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

public class Main {
    public static long numQueries = 0;
    private static int CASSANDRA_PORT = 9042;

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
        CqlSession session = getCassandraSession(keyspace, ip, port);

        String fileName = parsedArguments.getOptionValue("f");

        runQueries(session, fileName);
    }

    private static CqlSession getCassandraSession(String keyspace, String ip, int port) {
        SessionBuilder rawSession = CqlSession.builder().withKeyspace(CqlIdentifier.fromCql(keyspace));

        if (ip.isEmpty()) {
            return ((CqlSessionBuilder) rawSession).build();
        } else {
            int actualPort = port == -1 ? CASSANDRA_PORT : port;
            return ((CqlSessionBuilder) rawSession)
                    .addContactPoint(new InetSocketAddress(ip, actualPort))
                    .withLocalDatacenter("dc1")
                    .build();
        }
    }

    private static void runQueries(CqlSession session, String queryFilename) throws Exception {
        File queryTxt = new File(queryFilename);

        Scanner scanner = new Scanner(queryTxt);
        BaseTransaction transaction;

        // TODO: Record the timings and pass to Statistics.
        List<Long> timeRecord = new ArrayList<>();

        long start, end, lStart, lEnd, lapse;

        start = System.nanoTime();
        while (scanner.hasNext()) {
            numQueries++;

            String line = scanner.nextLine();
            String[] parameters = line.split(",");
            switch (parameters[0]) {
                case "N":
                    transaction = new NewOrderTransaction(session, parameters);
                    break;
                case "P":
                    transaction = new PaymentTransaction(session, parameters);
                    break;
                case "D":
                    transaction = new DeliveryTransaction(session, parameters);
                    break;
                case "O":
                    transaction = new OrderStatusTransaction(session, parameters);
                    break;
                case "S":
                    transaction = new StockLevelTransaction(session, parameters);
                    break;
                case "I":
                    transaction = new PopularItemTransaction(session, parameters);
                    break;
                case "T":
                    transaction = new TopBalanceTransaction(session, parameters);
                    break;
                case "R":
                    transaction = new RelatedCustomerTransaction(session, parameters);
                    break;
                default:
                    numQueries--;
                    throw new Exception("Unknown transaction types");
            }

            int moreLines = transaction.getExtraLines();
            String[] lines = new String[moreLines];
            for (int i = 0; i < moreLines; i++) {
                lines[i] = scanner.nextLine();
            }
            lStart = System.nanoTime();
            System.out.println("\n======================================================================");
            // System.out.printf("Transaction ID: %d\n", timeRecord.size());
            System.out.printf("Transaction ID: %d\n", numQueries);
            transaction.execute(lines);

            lEnd = System.nanoTime();
            lapse = lEnd - lStart;
            System.out.printf("Time taken: %d\n", TimeUnit.MILLISECONDS.convert(lapse, TimeUnit.NANOSECONDS));
            System.out.println("======================================================================");
        }
        session.close();
    }
}
