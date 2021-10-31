package cs4224.transactions;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import cs4224.utils.Constants;

import java.math.BigDecimal;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class DeliveryTransaction extends BaseTransaction {
    private static final int NO_OF_DISTRICTS = 10;
    private static final Format formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    PreparedStatement getOldestYtdOrderQuery;
    PreparedStatement updateOrderQuery;
    PreparedStatement updateOrderByCustomerQuery;
    PreparedStatement getOrderLinesQuery;
    PreparedStatement updateOrderLinesQuery;
    PreparedStatement getCustomerDetailsQuery;
    PreparedStatement updateCustomerDetailsQuery;

    public DeliveryTransaction(CqlSession session) {
        super(session);

        getOldestYtdOrderQuery = session.prepare(
                "SELECT * " +
                        "FROM orders " +
                        "WHERE O_W_ID = :o_w_id and O_D_ID = :o_d_id and O_CARRIER_ID = -1 " +
                        "LIMIT 1"
        );
        updateOrderQuery = session.prepare(
                "UPDATE orders " +
                        "SET O_CARRIER_ID = :o_carrier_id " +
                        "WHERE O_W_ID = :o_w_id AND O_D_ID = :o_d_id AND O_ID = :o_id " +
                        "IF O_CARRIER_ID = :null_delivery_id"
        );
        updateOrderByCustomerQuery = session.prepare(
                "UPDATE order_by_customer " +
                        "SET O_CARRIER_ID = :o_carrier_id " +
                        "WHERE C_W_ID = :c_w_id AND C_D_ID = :c_d_id " +
                        "AND C_ID = :c_id AND O_ID = :o_id"
        );
        getOrderLinesQuery = session.prepare(
                "SELECT OL_NUMBER, OL_AMOUNT " +
                        "FROM order_line " +
                        "WHERE OL_W_ID = :ol_w_id and OL_D_ID = :ol_d_id and OL_O_ID = :ol_o_id"
        );
        updateOrderLinesQuery = session.prepare(
                "UPDATE order_line " +
                        "SET OL_DELIVERY_D = :ol_delivery_d " +
                        "WHERE OL_W_ID = :ol_w_id and OL_D_ID = :ol_d_id and OL_O_ID = :ol_o_id and OL_NUMBER = :ol_number"
        );
        getCustomerDetailsQuery = session.prepare(
                "SELECT C_BALANCE, C_DELIVERY_CNT " +
                        "FROM customer " +
                        "WHERE C_W_ID = :c_w_id and C_D_ID = :c_d_id and C_ID = :c_id"
        );
        updateCustomerDetailsQuery = session.prepare(
                "UPDATE customer " +
                        "SET C_BALANCE = :c_balance, C_DELIVERY_CNT = :c_delivery_cnt " +
                        "WHERE C_W_ID = :c_w_id and C_D_ID = :c_d_id and C_ID = :c_id " +
                        "IF C_BALANCE = :original_c_balance"
        );

    }

    @Override
    public void execute(String[] dataLines, String[] parameters) {
        final int warehouseId = Integer.parseInt(parameters[1]);
        final int carrierId = Integer.parseInt(parameters[2]);

        List<Integer> districts = IntStream.rangeClosed(1, NO_OF_DISTRICTS).boxed().collect(Collectors.toList());

        districts.parallelStream().forEach(districtNo -> {
            Row row = getLatestOldestYtdOrder(warehouseId, districtNo);

            if (row == null) {
                System.out.printf("Skip district (%d, %d) as there is no undelivered order\n",
                        warehouseId, districtNo);
                return;
            }
            int orderId = row.getInt("O_ID");
            int customerId = row.getInt("O_C_ID");

            int numRetries = 0;
            boolean isApplied = false;

            while (numRetries < Constants.MAX_RETRIES && !isApplied) {
                ResultSet updateResult = updateOrder(warehouseId, districtNo, orderId, carrierId);
                isApplied = updateResult.wasApplied();
                if (!isApplied) {
                    row = getLatestOldestYtdOrder(warehouseId, districtNo);
                    orderId = row.getInt("O_ID");
                    customerId = row.getInt("O_C_ID");
                }
                numRetries++;
            }

            if (!isApplied) {
                System.out.printf("Skip order (%d, %d, %d) as it was processed by another delivery transaction\n",
                        warehouseId, districtNo, orderId);
                return;
            }

            updateOrderByCustomer(warehouseId, districtNo, orderId, customerId, carrierId);


            BigDecimal olAmount = new BigDecimal(0.0);
            List<Integer> ol_numbers = new ArrayList<>();
            List<Row> order_lines = session.execute(
                    getOrderLinesQuery
                            .boundStatementBuilder()
                            .setInt("ol_w_id", warehouseId)
                            .setInt("ol_d_id", districtNo)
                            .setInt("ol_o_id", orderId)
                            .build()
            ).all();

            for (Row ol : order_lines) {
                olAmount = olAmount.add(ol.getBigDecimal("OL_AMOUNT"));
                ol_numbers.add(ol.getInt("OL_NUMBER"));
            }

            Instant d = (new Date()).toInstant();
            int finalOrderId = orderId;
            ol_numbers.parallelStream().forEach(ol_number -> {
                session.execute(
                        updateOrderLinesQuery
                                .boundStatementBuilder()
                                .setInstant("ol_delivery_d", d)
                                .setInt("ol_w_id", warehouseId)
                                .setInt("ol_d_id", districtNo)
                                .setInt("ol_o_id", finalOrderId)
                                .setInt("ol_number", ol_number)
                                .build()
                );
            });

            Row cust = session.execute(
                    getCustomerDetailsQuery
                            .boundStatementBuilder()
                            .setInt("c_w_id", warehouseId)
                            .setInt("c_d_id", districtNo)
                            .setInt("c_id", customerId)
                            .build()
            ).one();

            numRetries = 0;
            isApplied = false;


            while (numRetries < Constants.MAX_RETRIES && !isApplied) {
                BigDecimal customerBalance = cust.getBigDecimal("C_BALANCE");
                int newDelCnt = cust.getInt("C_DELIVERY_CNT") + 1;
                ResultSet updateRes = updateCustomerDetails(warehouseId, districtNo, customerId, newDelCnt, customerBalance, olAmount);
                isApplied = updateRes.wasApplied();
                if (!isApplied) {
                    cust = updateRes.one();
                }
                numRetries++;
            }
        });
    }

    private ResultSet updateCustomerDetails(int warehouseId, int districtNo, int customerId, int newDelCnt,
                                            BigDecimal customerBalance, BigDecimal olAmount) {
        BigDecimal newAmount = customerBalance.add(olAmount);
        return session.execute(
                updateCustomerDetailsQuery
                        .boundStatementBuilder()
                        .setTimeout(Duration.ofSeconds(40))
                        .setBigDecimal("c_balance", newAmount)
                        .setInt("c_delivery_cnt", newDelCnt)
                        .setInt("c_w_id", warehouseId)
                        .setInt("c_d_id", districtNo)
                        .setInt("c_id", customerId)
                        .setBigDecimal("original_c_balance", customerBalance)
                        .build()
        );
    }

    private Row getLatestOldestYtdOrder(int warehouseId, int districtNo) {
        return session.execute(
                getOldestYtdOrderQuery
                        .boundStatementBuilder()
                        .setInt("o_w_id", warehouseId)
                        .setInt("o_d_id", districtNo)
                        .build()
        ).one();
    }

    private ResultSet updateOrder(int warehouseId, int districtNo, int orderId, int carrierId) {
        return session.execute(updateOrderQuery
                .boundStatementBuilder()
                .setInt("o_carrier_id", carrierId)
                .setInt("o_w_id", warehouseId)
                .setInt("o_d_id", districtNo)
                .setInt("o_id", orderId)
                .setInt("null_delivery_id", Constants.NULL_DELIVERY_ID)
                .build());
    }

    private void updateOrderByCustomer(int warehouseId, int districtNo, int orderId, int customerId, int carrierId) {
        session.execute(
                updateOrderByCustomerQuery
                        .boundStatementBuilder()
                        .setInt("o_carrier_id", carrierId)
                        .setInt("c_w_id", warehouseId)
                        .setInt("c_d_id", districtNo)
                        .setInt("c_id", customerId)
                        .setInt("o_id", orderId)
                        .build()
        );
    }

    @Override
    public String getType() {
        return "Delivery";
    }
}
