package cs4224.transactions;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import jnr.ffi.annotations.In;

import java.math.BigDecimal;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

public class DeliveryTransaction extends BaseTransaction{


    private static final int NO_OF_DISTRICTS = 10;
    private static final Format formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    public DeliveryTransaction(CqlSession session) {
        super(session);
    }

    @Override
    public void execute(String[] dataLines, String[] parameters) {
        final int warehouseId = Integer.parseInt(parameters[1]);
        final int carrierId = Integer.parseInt(parameters[2]);
        /*
            Given W_ID, CARRIER_ID
            For DISTRICT_NO from 1 to 10:
                a Find the oldest yet-to-be-delivered order -> order X , customer C
                b Update the order's carrier info in X
                c Update all the order-lines in X
                d Update customer C:
                    Increment C_BALANCE
                    Increment C_DELIVERY_CNT
         */

        for (int districtNo = 1; districtNo <= NO_OF_DISTRICTS; districtNo++){
            // a) Find the smallest order
            String queryA = String.format("SELECT * FROM orders WHERE O_W_ID = %d and O_D_ID = %d", warehouseId, districtNo);
            List<Row> orders = session.execute(queryA).all();
            int min_X = Integer.MAX_VALUE;
            int min_C = -1;
            for (Row order : orders) {
                if (order.isNull("O_CARRIER_ID")) {
                    int oid = order.getInt("O_ID");
                    if (oid < min_X) {
                        min_X = oid;
                        min_C = order.getInt("O_C_ID");
                    }

                }
            }

            if (min_C != -1) {
                //System.out.printf("Order no: %d, customer number: %d \n", min_X, min_C);
                //b) Update order X O_CARRIER_ID
                String queryB = String.format(
                        "UPDATE orders " +
                        "SET O_CARRIER_ID = %d " +
                        "WHERE O_W_ID = %d AND O_D_ID = %d AND O_ID = %d",
                        carrierId, warehouseId, districtNo, min_X);

                session.execute(queryB);
                System.out.printf("Will update carrier using query %s\n", queryB);


                Double olAmount = 0.0;
                List<Integer> ol_numbers = new ArrayList<>();


                String queryFindOrderlines = String.format(
                        "SELECT OL_NUMBER, OL_AMOUNT " +
                        "FROM order_line " +
                        "WHERE OL_W_ID = %d and OL_D_ID = %d and OL_O_ID = %d",
                        warehouseId, districtNo, min_X);
                List<Row> order_lines = session.execute(queryFindOrderlines).all();

                for (Row ol : order_lines) {
                    olAmount = olAmount + ol.getBigDecimal("OL_AMOUNT").doubleValue();
                    ol_numbers.add(ol.getInt("OL_NUMBER"));
                }

                //c) Update all the order-lines in X
                String d = formatter.format(new Date());
                for (int ol_number : ol_numbers) {
                    String queryC = String.format(
                            "UPDATE order_line " +
                                    "SET OL_DELIVERY_D = '%s' " +
                                    "WHERE OL_W_ID = %d and OL_D_ID = %d and OL_O_ID = %d and OL_NUMBER = %d",
                            d, warehouseId, districtNo, min_X, ol_number);

                    session.execute(queryC);

                    System.out.printf("Will update timestamp using query %s\n", queryC);
                }
                //d) Update customer C
//                String queryDGetOrderLinesTotalAmount = String.format(
//                        "SELECT SUM(OL_AMOUNT) AS amt " +
//                        "FROM order_line " +
//                        "WHERE OL_W_ID = %d and OL_D_ID = %d and OL_O_ID = %d",
//                        warehouseId, districtNo, min_X);

//                Row amt = session.execute(queryDGetOrderLinesTotalAmount).one();

                String queryDFindCustomer = String.format(
                        "SELECT C_BALANCE, C_DELIVERY_CNT " +
                        "FROM customer " +
                        "WHERE C_W_ID = %d and C_D_ID = %d and C_ID = %d",
                        warehouseId, districtNo, min_C);
                Row cust = session.execute(queryDFindCustomer).one();
                System.out.printf("Customer: %d, C_BALANCE: %f, C_DELIVERY_CNT: %d \n",
                        min_C, cust.getBigDecimal("C_BALANCE"), cust.getInt("C_DELIVERY_CNT"));

                Double newAmount = cust.getBigDecimal("C_BALANCE").doubleValue() + olAmount;
                int newDelCnt = cust.getInt("C_DELIVERY_CNT") + 1;

                String queryDUpdateCustomer = String.format(
                        "UPDATE customer " +
                        "SET C_BALANCE = %f, C_DELIVERY_CNT = %d " +
                        "WHERE C_W_ID = %d and C_D_ID = %d and C_ID = %d",
                        newAmount, newDelCnt, warehouseId, districtNo, min_C);

                session.execute(queryDUpdateCustomer);
                System.out.printf("Will update using query %s\n", queryDUpdateCustomer);

            }

        }

        System.out.printf("Running Delivery Transaction with W_ID=%d , CAREER_ID= %d \n", warehouseId, carrierId);
        Arrays.stream(dataLines).forEach(System.out::println);
    }
}
