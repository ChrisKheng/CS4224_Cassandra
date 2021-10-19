package cs4224.transactions;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;

import java.text.Format;
import java.text.SimpleDateFormat;
import java.time.Instant;
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


        for (int districtNo = 1; districtNo <= NO_OF_DISTRICTS; districtNo++) {
            // a) Find the smallest order

            String queryA = String.format(
                    "SELECT * " +
                    "FROM orders " +
                    "WHERE O_W_ID = %d and O_D_ID = %d and O_CARRIER_ID = -1 " +
                    "LIMIT 1",
                    warehouseId, districtNo);
            Row row = session.execute(queryA).one();


            if (row != null) {
                int orderId = row.getInt("O_ID");
                int customerId = row.getInt("O_C_ID");

                //b) Update order X O_CARRIER_ID
                String queryB = String.format(
                        "UPDATE orders " +
                        "SET O_CARRIER_ID = %d " +
                        "WHERE O_W_ID = %d AND O_D_ID = %d AND O_ID = %d",
                        carrierId, warehouseId, districtNo, orderId);

                session.execute(queryB);

                //TODO: turn all statements in this file into PreparedStatement
                session.execute(
                        session.prepare(
                                "UPDATE order_by_customer " +
                                        "SET O_CARRIER_ID = :o_carrier_id " +
                                        "WHERE C_W_ID = :c_w_id AND C_D_ID = :c_d_id " +
                                        "AND C_ID = :c_id AND O_ID = :o_id"
                                )
                                .boundStatementBuilder()
                                .setInt("o_carrier_id", carrierId)
                                .setInt("c_w_id", warehouseId)
                                .setInt("c_d_id", districtNo)
                                .setInt("c_id", customerId)
                                .setInt("o_id", orderId)
                                .build()
                );


                Double olAmount = 0.0;
                List<Integer> ol_numbers = new ArrayList<>();


                String queryFindOrderlines = String.format(
                        "SELECT OL_NUMBER, OL_AMOUNT " +
                        "FROM order_line " +
                        "WHERE OL_W_ID = %d and OL_D_ID = %d and OL_O_ID = %d",
                        warehouseId, districtNo, orderId);

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
                            d, warehouseId, districtNo, orderId, ol_number);

                    session.execute(queryC);

                    //System.out.printf("Will update timestamp using query %s\n", queryC);
                }
                //d) Update customer C

                String queryDFindCustomer = String.format(
                        "SELECT C_BALANCE, C_DELIVERY_CNT " +
                        "FROM customer " +
                        "WHERE C_W_ID = %d and C_D_ID = %d and C_ID = %d",
                        warehouseId, districtNo, customerId);
                Row cust = session.execute(queryDFindCustomer).one();
                //System.out.printf("Customer: %d, C_BALANCE: %f, C_DELIVERY_CNT: %d \n",
                //        customerId, cust.getBigDecimal("C_BALANCE"), cust.getInt("C_DELIVERY_CNT"));


                Double newAmount = cust.getBigDecimal("C_BALANCE").doubleValue() + olAmount;
                int newDelCnt = cust.getInt("C_DELIVERY_CNT") + 1;

                String queryDUpdateCustomer = String.format(
                        "UPDATE customer " +
                        "SET C_BALANCE = %f, C_DELIVERY_CNT = %d " +
                        "WHERE C_W_ID = %d and C_D_ID = %d and C_ID = %d",
                        newAmount, newDelCnt, warehouseId, districtNo, customerId);

                session.execute(queryDUpdateCustomer);
                //System.out.printf("Will update using query %s\n", queryDUpdateCustomer);
            }

        }

        //System.out.printf("Running Delivery Transaction with W_ID=%d , CAREER_ID= %d \n", warehouseId, carrierId);
        Arrays.stream(dataLines).forEach(System.out::println);

    }

    @Override
    public String getType() {
        return "Delivery";
    }
}
