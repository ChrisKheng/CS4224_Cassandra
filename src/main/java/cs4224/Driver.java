package cs4224;

import com.datastax.oss.driver.api.core.CqlSession;
import com.google.inject.Inject;
import cs4224.transactions.*;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

public class Driver {
    public static long numQueries = 0;

    private final CqlSession session;
    private final NewOrderTransaction newOrderTransaction;
    private final PaymentTransaction paymentTransaction;
    private final DeliveryTransaction deliveryTransaction;
    private final OrderStatusTransaction orderStatusTransaction;
    private final StockLevelTransaction stockLevelTransaction;
    private final PopularItemTransaction popularItemTransaction;
    private final TopBalanceTransaction topBalanceTransaction;
    private final RelatedCustomerTransaction relatedCustomerTransaction;


    @Inject
    public Driver(CqlSession session, NewOrderTransaction newOrderTransaction, PaymentTransaction paymentTransaction,
                  DeliveryTransaction deliveryTransaction, OrderStatusTransaction orderStatusTransaction,
                  StockLevelTransaction stockLevelTransaction, PopularItemTransaction popularItemTransaction,
                  TopBalanceTransaction topBalanceTransaction, RelatedCustomerTransaction relatedCustomerTransaction) {
        this.session = session;
        this.newOrderTransaction = newOrderTransaction;
        this.paymentTransaction = paymentTransaction;
        this.deliveryTransaction = deliveryTransaction;
        this.orderStatusTransaction = orderStatusTransaction;
        this.stockLevelTransaction = stockLevelTransaction;
        this.popularItemTransaction = popularItemTransaction;
        this.topBalanceTransaction = topBalanceTransaction;
        this.relatedCustomerTransaction = relatedCustomerTransaction;
    }

    void runQueries(String queryFilename) throws Exception {
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
                    transaction = newOrderTransaction;
                    break;
                case "P":
                    transaction = paymentTransaction;
                    break;
                case "D":
                    transaction = deliveryTransaction;
                    break;
                case "O":
                    transaction = orderStatusTransaction;
                    break;
                case "S":
                    transaction = stockLevelTransaction;
                    break;
                case "I":
                    transaction = popularItemTransaction;
                    break;
                case "T":
                    transaction = topBalanceTransaction;
                    break;
                case "R":
                    transaction = relatedCustomerTransaction;
                    break;
                default:
                    numQueries--;
                    // throw new Exception("Unknown transaction types");
                    System.err.println("Unknown transaction types");
                    continue;
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
            transaction.execute(lines, parameters);

            lEnd = System.nanoTime();
            lapse = lEnd - lStart;
            System.out.printf("Time taken: %d\n", TimeUnit.MILLISECONDS.convert(lapse, TimeUnit.NANOSECONDS));
            System.out.println("======================================================================");
        }
        session.close();
    }
}
