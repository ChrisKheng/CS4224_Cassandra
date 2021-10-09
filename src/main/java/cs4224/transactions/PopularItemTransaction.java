package cs4224.transactions;

import com.datastax.oss.driver.api.core.CqlSession;
import cs4224.dao.*;
import cs4224.entities.*;

import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PopularItemTransaction extends BaseTransaction {
    private final DistrictDao districtDao;
    private final CustomerDao customerDao;
    private final OrderDao orderDao;
    private final OrderLineDao orderLineDao;
    private final ItemDao itemDao;
    private final OrderByItemDao orderByItemDao;

    public PopularItemTransaction(CqlSession session, DistrictDao districtDao, CustomerDao customerDao,
                                  OrderDao orderDao, OrderLineDao orderLineDao, ItemDao itemDao,
                                  OrderByItemDao orderByItemDao) {
        super(session);
        this.districtDao = districtDao;
        this.customerDao = customerDao;
        this.orderDao = orderDao;
        this.orderLineDao = orderLineDao;
        this.itemDao = itemDao;
        this.orderByItemDao = orderByItemDao;
    }

    @Override
    public void execute(String[] dataLines, String[] parameters) {
        final int warehouseId = Integer.parseInt(parameters[1]);
        final int districtId = Integer.parseInt(parameters[2]);
        final int L = Integer.parseInt(parameters[3]);

        final Integer nextOrderId = getDistrict(warehouseId, districtId).getNextOrderId();
        final List<Order> orders = orderDao.getById(warehouseId, districtId,
                nextOrderId - L, nextOrderId).all().stream()
                .map(Order::map).collect(Collectors.toList());

        final Map<Order, BigDecimal> orderQuantity = new HashMap<>(); // Order -> max_quantity
        final Map<Integer, List<Integer>> orderItems = new HashMap<>(); // OrderId -> items
        final Map<Integer, Customer> customerMap = new HashMap<>(); // CustomerId -> CustomerName

        orders.parallelStream().forEach(order -> {
            final BigDecimal max_quantity = getOrderLineMaxQuantity(warehouseId, districtId, order);
            final Stream<OrderLine> orderLineItem = getOrderLine(warehouseId, districtId, order, max_quantity);
            final List<Integer> items = orderLineItem.map(OrderLine::getItemId).collect(Collectors.toList());
            orderQuantity.put(order, max_quantity);
            orderItems.put(order.getId(), items);
            customerMap.put(order.getCustomerId(),
                    Customer.map(customerDao.getNameById(warehouseId, districtId, order.getCustomerId())));
        });


        final Set<Integer> items = new HashSet<>(); // Set of all Items in last L orders
        final Map<Integer, Long> itemNumOrders = new HashMap<>(); // Item -> Num of Orders
        final Map<Integer, String> itemName = new HashMap<>(); // Item -> Item Name

        orderItems.values().forEach(items::addAll);
        final List<Integer> orderIds = orders.stream()
                .map(Order::getId).collect(Collectors.toList()); // List of L orderIds

        items.parallelStream().forEach(item -> {
            itemNumOrders.put(item, orderByItemDao.getCountByItemId(warehouseId, districtId, item, orderIds));
            itemName.put(item, getItemName(item));
        });

        printOutput(warehouseId, districtId, L, orderQuantity, orderItems, customerMap, itemNumOrders, itemName);
    }

    private District getDistrict(final int warehouseId, final int districtId) {
        return District.map(districtDao.getNextOrderId(warehouseId, districtId));
    }

    private BigDecimal getOrderLineMaxQuantity(final int warehouseId, final int districtId, final Order order) {
        return OrderLine.map(orderLineDao
                .getOLQuantity(warehouseId, districtId, order.getId())).getQuantity();
    }

    private Stream<OrderLine> getOrderLine(final int warehouseId, final int districtId, final Order order,
                                           final BigDecimal max_quantity) {
        return orderLineDao.getOLItemId(warehouseId, districtId, order.getId(), max_quantity).all()
                .stream().map(OrderLine::map);
    }

    private String getItemName(final int item) {
        return Item.map(itemDao.getNameById(item)).getName();
    }

    private void printOutput(final int warehouseId, final int districtId, final int L,
                             final Map<Order, BigDecimal> orderQuantity, final Map<Integer, List<Integer>> orderItems,
                             final Map<Integer, Customer> customerMap, final Map<Integer, Long> itemPopularity,
                             final Map<Integer, String> itemName) {
        System.out.printf(" Warehouse Id: %d, District Id: %d\n", warehouseId, districtId);
        System.out.printf(" Number of last orders to be examined: %d\n\n", L);

        orderQuantity.forEach((order, quantity) -> {
            System.out.printf(" Order number: %d, Entry date and time: %s\n", order.getId(), order.getEntryDateTime());
            System.out.printf(" Customer: %s\n", customerMap.get(order.getCustomerId()));
            orderItems.get(order.getId()).forEach(item ->
                    System.out.printf(" Item Name: %s, Order Line Quantity: %f\n", itemName.get(item), quantity));
            System.out.println();
        });

        itemPopularity.forEach((item, numOrders) ->
                System.out.printf(" Item Name: %s, Percentage orders : %f\n", itemName.get(item),
                        ((float) numOrders / L)));
    }
}
