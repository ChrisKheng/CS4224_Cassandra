package cs4224.transactions;

import com.datastax.oss.driver.api.core.CqlSession;
import com.sun.tools.javac.util.Pair;
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

        // Order -> (max_quantity, [popular items])
        final Map<Order, Pair<BigDecimal, List<Integer>>> orderPopularItems = new HashMap<>();

        // CustomerId -> CustomerName
        final Map<Integer, Customer> customerMap = new HashMap<>();

        orders.forEach(order -> {
            final BigDecimal max_quantity = getOrderLineMaxQuantity(warehouseId, districtId, order);
            final Stream<OrderLine> orderLineItem = getOrderLine(warehouseId, districtId, order, max_quantity);
            final List<Integer> items = orderLineItem.map(OrderLine::getItemId).collect(Collectors.toList());
            orderPopularItems.put(order, new Pair<>(max_quantity, items));

            customerMap.put(order.getCustomerId(),
                    Customer.map(customerDao.getNameById(warehouseId, districtId, order.getCustomerId())));
        });

        // All Items in last L orders
        final Set<Integer> items = new HashSet<>();
        orderPopularItems.forEach((order, pair) -> items.addAll(pair.snd));

        // List of L orderIds
        final List<Integer> orderIds = orders.stream().map(Order::getId).collect(Collectors.toList());

        // Item -> Num of Orders
        final Map<Integer, Long> itemPopularity = new HashMap<>();
        // Item -> Item Name
        final Map<Integer, String> itemName = new HashMap<>();
        items.forEach(item -> {
            itemPopularity.put(item, orderByItemDao.getCountByItemId(warehouseId, districtId, item, orderIds));
            itemName.put(item, getItemName(item));
        });

        printOutput(warehouseId, districtId, L, orderPopularItems, customerMap, itemPopularity, itemName);
    }


    private void printOutput(final int warehouseId, final int districtId, final int L,
                             final Map<Order, Pair<BigDecimal, List<Integer>>> orderPopularItems,
                             final Map<Integer, Customer> customerMap, final Map<Integer, Long> itemPopularity,
                             final Map<Integer, String> itemName) {
        System.out.printf(" Warehouse Id: %d, District Id: %d\n", warehouseId, districtId);
        System.out.printf(" Number of last orders to be examined: %d\n\n", L);

        orderPopularItems.forEach((order, pair) -> {
            System.out.printf(" Order number: %d, Entry date and time: %s\n", order.getId(), order.getEntryDateTime());
            System.out.printf(" Customer: %s\n", customerMap.get(order.getCustomerId()));
            pair.snd.forEach(item ->
                    System.out.printf(" Item Name: %s, Order Line Quantity: %f\n", itemName.get(item), pair.fst));
            System.out.println();
        });

        itemPopularity.forEach((item, numOrders) ->
                System.out.printf(" Item Name: %s, Percentage orders : %f\n", itemName.get(item),
                        ((float) numOrders / L)));
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
}
