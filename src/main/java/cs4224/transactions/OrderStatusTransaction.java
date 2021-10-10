package cs4224.transactions;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class OrderStatusTransaction extends BaseTransaction {

    public OrderStatusTransaction(final CqlSession session) {
        super(session);
    }

    @Override
    public void execute(String[] dataLines,  String[] parameters) {
        final int warehouseId = Integer.parseInt(parameters[1]);
        final int districtId = Integer.parseInt(parameters[2]);
        final int customerId = Integer.parseInt(parameters[3]);

        /*
            1) Find customer's name, balance
            2) Find info of customer's last order
            3) For each item in the customer's last order, find info
         */
        String queryGetCustomerInfo = String.format(
                "SELECT C_FIRST, C_MIDDLE, C_LAST, C_BALANCE " +
                "FROM customer " +
                "WHERE C_W_ID = %d and C_D_ID = %d and C_ID = %d", warehouseId, districtId, customerId);


        Row customer = session.execute(queryGetCustomerInfo).one();

        if (customer != null) {
            // 1
            String first = customer.getString("C_FIRST");
            String middle = customer.getString("C_MIDDLE");
            String last = customer.getString("C_LAST");
            Double balance = customer.getBigDecimal("C_BALANCE").doubleValue();
            System.out.printf("First: %s, second: %s, last: %s \nBalance: %f \n", first, middle, last, balance);

            //2
            // order_by_customer does not seem any faster here
            String queryGetCustomerLastOrder = String.format(
                    "SELECT O_ID, O_ENTRY_D, O_CARRIER_ID FROM orders " +
                            "WHERE O_W_ID = %d and O_D_ID = %d and O_C_ID = %d" +
                            "ORDER BY O_ID DESC LIMIT 1 ALLOW FILTERING", warehouseId, districtId, customerId);

            Row lastOrder = session.execute(queryGetCustomerLastOrder).one();

            if (lastOrder != null ) {
                int orderNumber = lastOrder.getInt("O_ID");
                String entryTime = lastOrder.getInstant("O_ENTRY_D").toString();
                int carrierId = lastOrder.getInt("O_CARRIER_ID");
                System.out.printf("Last order's ID: %d, entry time: %s, carrier's ID: %s \n",
                        orderNumber, entryTime, carrierId > -1 ? String.valueOf(carrierId) : "null [Order has not been delivered]");

                String queryGetItemsFromLastOrder = String.format(
                        "SELECT OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D " +
                        "FROM order_line " +
                        "WHERE OL_W_ID = %d and OL_D_ID = %d and OL_O_ID = %d", warehouseId, districtId, orderNumber);

                List<Row> items = session.execute(queryGetItemsFromLastOrder).all();
                for (Row item : items) {
                    int itemId = item.getInt("OL_I_ID");
                    int supplyWarehouseId = item.getInt("OL_SUPPLY_W_ID");
                    double quantity = item.getBigDecimal("OL_QUANTITY").doubleValue();
                    double amount = item.getBigDecimal("OL_AMOUNT").doubleValue();

                    Instant dDate = item.getInstant("OL_DELIVERY_D");

                    String deliveryDate = dDate == null ? "NA" : dDate.toString();

                    System.out.printf(
                            "Item ID : %s, supply warehouse ID: %s, quantity: %f, amount: %f delivery date: %s \n",
                            itemId, supplyWarehouseId, quantity, amount, deliveryDate);
                }
            } else {
                System.out.println("Customer has no complete order.");
            }

        } else {
            System.out.println("Customer not found.");
        }
    }
}
