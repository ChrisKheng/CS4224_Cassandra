package cs4224;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;

import cs4224.utils.DataLoader;
import cs4224.utils.Statistics;

import cs4224.transactions.BaseTransaction;
import cs4224.transactions.DeliveryTransaction;
import cs4224.transactions.NewOrderTransaction;
import cs4224.transactions.OrderStatusTransaction;
import cs4224.transactions.PaymentTransaction;
import cs4224.transactions.PopularItemTransaction;
import cs4224.transactions.RelatedCustomerTransaction;
import cs4224.transactions.StockLevelTransaction;
import cs4224.transactions.TopBalanceTransaction;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class Main {

    public static void main(String[] args) {
	    // write your code here
        System.out.println("[START OF PROGRAM]");
        try {
            run(args);

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
        System.out.println("[END OF PROGRAM]");
    }

    public static void run(String[] args) throws Exception {
        if (args.length != 0) {
            switch (args[0].toLowerCase()) {
                case "setupdb":
                    System.out.println("[Setup Database]");
                    try (DataLoader loader = new DataLoader()) {
                        //loader.loadSchema();
                        System.out.println("====Create Tables ");
                        loader.loadData();
                        System.out.println("====Inject Data");
                    }
                    break;

                case "run_queries":
                    System.out.println("[Running queries]");
                    runQueries(args);
                    break;

                default:
                    throw new Exception("Invalid command.");
            }
        }
    }


    public static void runQueries(String[] args) throws Exception {
        CqlSession session = CqlSession.builder().
                withKeyspace(CqlIdentifier.fromCql("wholesale")).
                build();

        File queryTxt = new File(args[1]);

        Scanner scanner = new Scanner(queryTxt);
        BaseTransaction transaction;

        // TODO: Record the timings and pass to Statistics.
        List<Long> timeRecord = new ArrayList<>();

        long start, end, lStart, lEnd, lapse;

        start = System.nanoTime();
        while (scanner.hasNext()) {

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
                    throw new Exception("Unknown transaction types");
            }

            int moreLines = transaction.getExtraLines();
            String[] lines = new String[moreLines];
            for (int i = 0; i < moreLines; i++) {
                lines[i] = scanner.nextLine();
            }
            lStart = System.nanoTime();
            System.out.println("\n======================================================================");
            System.out.printf("Transaction ID: %d\n", timeRecord.size());
            transaction.execute(lines);

            lEnd = System.nanoTime();
            lapse = lEnd - lStart;
            System.out.printf("Time taken: %d\n", lapse);
            System.out.println("======================================================================");
        }
        session.close();
    }
}
