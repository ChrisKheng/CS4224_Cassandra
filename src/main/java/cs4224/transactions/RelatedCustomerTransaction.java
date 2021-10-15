package cs4224.transactions;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import cs4224.ParallelExecutor;
import cs4224.entities.Customer;
import cs4224.entities.Order;

import java.util.*;

public class RelatedCustomerTransaction extends BaseTransaction {
    PreparedStatement getOrdersOfCustomerQuery;
    PreparedStatement getItemsOfOrderQuery;
    PreparedStatement getOrdersOfItemQuery;
    PreparedStatement getCustomerOfOrderQuery;

    public RelatedCustomerTransaction(CqlSession session) {
        super(session);

        getOrdersOfCustomerQuery = session.prepare(
                "SELECT O_ID "
                        + "FROM order_by_customer "
                        + "WHERE C_W_ID = :c_w_id AND C_D_ID = :c_d_id AND C_ID = :c_id"
        );
        getItemsOfOrderQuery = session.prepare(
                "SELECT OL_I_ID "
                        + "FROM order_line "
                        + "WHERE OL_W_ID = :ol_w_id AND OL_D_ID = :ol_d_id AND OL_O_ID = :ol_o_id"
        );
        getOrdersOfItemQuery = session.prepare(
                "SELECT O_W_ID, O_D_ID, O_ID "
                        + "FROM order_by_item "
                        + "WHERE I_ID = :i_id"
        );
        getCustomerOfOrderQuery = session.prepare(
                "SELECT O_C_ID "
                        + "FROM orders "
                        + "WHERE O_W_ID = :ol_w_id AND O_D_ID = :ol_d_id AND O_ID = :o_id"
        );
    }

    @Override
    public void execute(String[] dataLines, String[] parameters) {
        final int customerWarehouseId = Integer.parseInt(parameters[1]);
        final int customerDistrictId = Integer.parseInt(parameters[2]);
        final int customerId = Integer.parseInt(parameters[3]);

        HashSet<Customer> relatedCustomers = executeAndGetResult(customerWarehouseId, customerDistrictId, customerId);
        System.out.printf("Number of relatedCustomers: %d\n", relatedCustomers.size());
        System.out.printf("Related customers (C_W_ID, C_D_ID, C_ID):");
        int count = 1;
        for (Customer customer : relatedCustomers) {
            if (count == relatedCustomers.size()) {
                System.out.printf(" %s", customer.toSpecifier());
            } else {
                System.out.printf(" %s,", customer.toSpecifier());
            }
            count++;
        }
        System.out.printf("\n");
    }

    public HashSet<Customer> executeAndGetResult(int customerWarehouseId, int customerDistrictId, int customerId) {
        // 1. Select all the orders that belong to the given customer.
        ResultSet orderIds = session.execute(getOrdersOfCustomerQuery.boundStatementBuilder()
                .setInt("c_w_id", customerWarehouseId)
                .setInt("c_d_id", customerDistrictId)
                .setInt("c_id", customerId)
                .build());
        Set<Order> relatedOrders = Collections.synchronizedSet(new HashSet<>());

        // 2. For each order retrieved in 1, get the list of related customers.
        // Probably can optimize by avoiding rescanning repeated potentially related order
        List<Integer> oids = new ArrayList<>();
        for (Row orderIdRow : orderIds) {
            oids.add(orderIdRow.getInt("O_ID"));
        }
        oids.parallelStream().forEach(oid -> {
            Order order = Order.builder()
                    .warehouseId(customerWarehouseId)
                    .districtId(customerDistrictId)
                    .id(oid)
                    .build();
            HashSet<Order> result = getRelatedOrders(order);
            relatedOrders.addAll(result);
        });

        return getCustomersOfOrders(relatedOrders);
    }

    private HashSet<Order> getRelatedOrders(Order order) {
        HashSet<Order> relatedOrders = new HashSet<>();

        // 2.1. Select all the items that belong to the order.
        ResultSet itemIds = session.execute(getItemsOfOrderQuery.boundStatementBuilder()
                .setInt("ol_w_id", order.getWarehouseId())
                .setInt("ol_d_id", order.getDistrictId())
                .setInt("ol_o_id", order.getId())
                .build());
        HashSet<Integer> itemIdsSet = new HashSet<>();
        itemIds.forEach(r -> itemIdsSet.add(r.getInt("OL_I_ID")));

        // 2.2. For each item retrieved in 2.1:
        for (Integer itemId : itemIdsSet) {
            // 2.2.1 Select all the orders that have the item
            ResultSet potentialOrders = session.execute(getOrdersOfItemQuery
                    .boundStatementBuilder()
                    .setInt("i_id", itemId)
                    .build());

            // 2.2.2. For each potentially related order retrieved in 2.2.1:
            for (Row potentialOrderRow : potentialOrders) {
                // Ignore the order if it belongs to the same warehouse as the given customer.
                if (potentialOrderRow.getInt("O_W_ID") == order.getWarehouseId()) {
                    continue;
                }

                int potentialOrderWid = potentialOrderRow.getInt("O_W_ID");
                int potentialOrderDid = potentialOrderRow.getInt("O_D_ID");
                int potentialOrderId = potentialOrderRow.getInt("O_ID");

                // 2.2.2.1 Select all the items of the potentially related order.
                ResultSet potentialOrderItems = session.execute(getItemsOfOrderQuery.boundStatementBuilder()
                        .setInt("ol_w_id", potentialOrderWid)
                        .setInt("ol_d_id", potentialOrderDid)
                        .setInt("ol_o_id", potentialOrderId)
                        .build());

                // 2.2.2.2 Add the order in 2.2.2 to the result set if it contains at least one other distinct common
                // item with the given order to the function.
                for (Row potentialOrderItemRow : potentialOrderItems) {
                    int potentialOrderItemId = potentialOrderItemRow.getInt("OL_I_ID");
                    if (potentialOrderItemId != itemId && itemIdsSet.contains(potentialOrderItemId)) {
                        relatedOrders.add(
                                Order.builder()
                                        .warehouseId(potentialOrderWid)
                                        .districtId(potentialOrderDid)
                                        .id(potentialOrderId)
                                        .build());
                        break;
                    }
                }
            }
        }

        return relatedOrders;
    }

    private HashSet<Customer> getCustomersOfOrders(Set<Order> orders) {
        HashSet<Customer> customers = new HashSet<>();

        for (Order order : orders) {
            ResultSet customerIdRow = session.execute(getCustomerOfOrderQuery.boundStatementBuilder()
                    .setInt("ol_w_id", order.getWarehouseId())
                    .setInt("ol_d_id", order.getDistrictId())
                    .setInt("o_id", order.getId())
                    .build());
            customers.add(Customer.builder()
                    .warehouseId(order.getWarehouseId())
                    .districtId(order.getDistrictId())
                    .id(customerIdRow.one().getInt("O_C_ID"))
                    .build());
        }

        return customers;
    }
}
