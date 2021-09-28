package cs4224.transactions;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import cs4224.entities.Customer;
import cs4224.entities.Order;

import java.util.HashSet;
import java.util.Objects;

public class RelatedCustomerTransaction extends BaseTransaction {
    private final int customerWarehouseId;
    private final int customerDistrictId;
    private final int customerId;

    public RelatedCustomerTransaction(CqlSession session, String[] parameters) {
        super(session, parameters);
        customerWarehouseId = Integer.parseInt(parameters[1]);
        customerDistrictId = Integer.parseInt(parameters[2]);
        customerId = Integer.parseInt(parameters[3]);
    }

    @Override
    public void execute(String[] dataLines) {
        // 1. Select all the orders that belong to the given customer.
        String query1 = String.format(
                "Select O_ID "
                        + "FROM order_by_customer "
                        + "WHERE C_W_ID = %d AND C_D_ID = %d AND C_ID = %d"
                , customerWarehouseId, customerDistrictId, customerId);
        ResultSet orderIds = session.execute(query1);
        HashSet<Order> relatedOrders = new HashSet<>();

        // 2. For each order retrieved in 1:
        // Probably can optimize by avoiding rescanning repeated potentially related order
        for (Row orderIdRow : orderIds) {
            // 2.1. Select all the items that belong to the order.
            int orderId = orderIdRow.getInt("O_ID");
            String query2 = String.format(
                    "SELECT OL_I_ID "
                    + "FROM order_line "
                    + "WHERE OL_W_ID = %d AND OL_D_ID = %d AND OL_O_ID = %d"
            , customerWarehouseId, customerDistrictId, orderId);
            ResultSet itemIds = session.execute(query2);
            HashSet<Integer> itemIdsSet = new HashSet<>();
            itemIds.forEach(r -> itemIdsSet.add(r.getInt("OL_I_ID")));

            // 2.2. For each item retrieved in 2.1:
            // This for loop can be moved out of this nested for loop.
            for (Integer itemId : itemIdsSet) {
                // 2.2.1 Select all the orders that have the item
                String query3 = String.format(
                        "SELECT O_W_ID, O_D_ID, O_ID "
                        + "FROM order_by_item "
                        + "WHERE I_ID = %d"
                , itemId);
                ResultSet potentialOrders = session.execute(query3);

                // 2.2.2. For each potentially related order retrieved in 2.2.1:
                for (Row potentialOrderRow : potentialOrders) {
                    // Ignore the order if it belongs to the same warehouse as the given customer.
                    if (potentialOrderRow.getInt("O_W_ID") == customerWarehouseId) {
                        continue;
                    }

                    int potentialOrderWid = potentialOrderRow.getInt("O_W_ID");
                    int potentialOrderDid = potentialOrderRow.getInt("O_D_ID");
                    int potentialOrderId = potentialOrderRow.getInt("O_ID");

                    // 2.2.2.1 Select all the items of the potentially related order.
                    String query4 = String.format(
                            "SELECT OL_I_ID "
                            + "FROM order_line "
                            + "WHERE OL_W_ID = %d AND OL_D_ID = %d AND OL_O_ID = %d"
                    , potentialOrderWid, potentialOrderDid, potentialOrderId);
                    ResultSet potentialOrderItems = session.execute(query4);

                    // 2.2.2.2
                    for (Row potentialOrderItemRow : potentialOrderItems) {
                        int potentialOrderItemId = potentialOrderItemRow.getInt("OL_I_ID");
                        if (potentialOrderItemId != itemId && itemIdsSet.contains(potentialOrderItemId)) {
                            relatedOrders.add(
                                    Order.builder()
                                    .id(potentialOrderId)
                                    .warehouseId(potentialOrderWid)
                                    .districtId(potentialOrderDid)
                                    .build());
                            break;
                        }
                    }
                }
            }
        }

        HashSet<Customer> relatedCustomers = getCustomersOfOrders(relatedOrders);
        System.out.printf("Number of relatedCustomers: %d\n", relatedCustomers.size());
        relatedCustomers.forEach(System.out::println);
    }

    private HashSet<Order> getRelatedOrders(Order order) {
        HashSet<Order> relatedOrders = new HashSet<>();
        return relatedOrders;
    }

    private HashSet<Customer> getCustomersOfOrders(HashSet<Order> orders) {
        HashSet<Customer> customers = new HashSet<>();

        for (Order order : orders) {
            String query = String.format(
                    "SELECT O_C_ID "
                    + "FROM orders "
                    + "WHERE O_W_ID = %d AND O_D_ID = %d AND O_ID = %d"
            , order.getWarehouseId(), order.getDistrictId(), order.getId());
            System.out.println(query);

            ResultSet customerIdRow = session.execute(query);
            customers.add(Customer.builder()
                    .id(customerIdRow.one().getInt("O_C_ID"))
                    .warehouseId(order.getWarehouseId())
                    .districtId(order.getDistrictId())
                    .build());
        }

        return customers;
    }
}
