package cs4224;

import com.google.inject.Inject;
import com.opencsv.CSVWriter;
import cs4224.dao.*;
import cs4224.entities.*;

import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;
import java.util.stream.Collectors;

public class DBState {

    private static final String STATS_FILE = "dbstate.csv";

    private final WarehouseDao warehouseDao;
    private final DistrictDao districtDao;
    private final CustomerDao customerDao;
    private final OrderDao orderDao;
    private final OrderLineDao orderLineDao;
    private final StockDao stockDao;

    @Inject
    public DBState(WarehouseDao warehouseDao, DistrictDao districtDao, CustomerDao customerDao,
                   OrderDao orderDao, OrderLineDao orderLineDao, StockDao stockDao) {
        this.warehouseDao = warehouseDao;
        this.districtDao = districtDao;
        this.customerDao = customerDao;
        this.orderDao = orderDao;
        this.orderLineDao = orderLineDao;
        this.stockDao = stockDao;
    }

    public void save() {
        final Warehouse warehouse = Warehouse.map(this.warehouseDao.getState());
        final District district = District.map(this.districtDao.getState());
        final Customer customer = Customer.map(this.customerDao.getState());
        final Order order = Order.map(this.orderDao.getState());

        final List<Warehouse> warehouseList = this.warehouseDao.getAllWarehouseIDs().all().stream()
                .map(Warehouse::map).collect(Collectors.toList());

        final OrderLine olTotal = new OrderLine().setAmount(new BigDecimal(0)).setQuantity(new BigDecimal(0));
        final Stock stockTotal = new Stock().setQuantity(new BigDecimal(0)).setYtdQuantity(new BigDecimal(0))
                .setRemoteOrderCount(0).setOrderCount(0);

        warehouseList.forEach(w -> {
            final OrderLine orderLine = OrderLine.map(this.orderLineDao.getState(w.getId()));
            olTotal.setAmount(olTotal.getAmount().add(orderLine.getAmount()));
            olTotal.setQuantity(olTotal.getQuantity().add(orderLine.getQuantity()));
            final Stock stock = Stock.map(this.stockDao.getState(w.getId()));
            stockTotal.setQuantity(stockTotal.getQuantity().add(stock.getQuantity()));
            stockTotal.setYtdQuantity(stockTotal.getYtdQuantity().add(stock.getYtdQuantity()));
            stockTotal.setRemoteOrderCount(stockTotal.getRemoteOrderCount() + stock.getRemoteOrderCount());
            stockTotal.setOrderCount(stockTotal.getOrderCount() + stock.getOrderCount());
        });

        try {
            FileWriter output = new FileWriter(STATS_FILE);
            CSVWriter writer = new CSVWriter(output, CSVWriter.DEFAULT_SEPARATOR, CSVWriter.NO_QUOTE_CHARACTER);

            writer.writeNext(new String[]{"Statistic", "Value"});
            writer.writeNext(new String[]{"sum(W_YTD)", warehouse.getAmountPaidYTD().toString()});
            writer.writeNext(new String[]{"sum(D_YTD)", district.getAmountPaidYTD().toString()});
            writer.writeNext(new String[]{"sum(D_NEXT_O_ID)", district.getNextOrderId().toString()});
            writer.writeNext(new String[]{"sum(C_BALANCE)", customer.getBalance().toString()});
            writer.writeNext(new String[]{"sum(C_YTD_PAYMENT)", customer.getPaymentYTD().toString()});
            writer.writeNext(new String[]{"sum(C_PAYMENT_CNT)", customer.getNumPayments().toString()});
            writer.writeNext(new String[]{"sum(C_DELIVERY_CNT)", customer.getNumDeliveries().toString()});
            writer.writeNext(new String[]{"max(O_ID)", order.getId().toString()});
            writer.writeNext(new String[]{"sum(O_OL_CNT)", order.getNumItems().toString()});
            writer.writeNext(new String[]{"sum(OL_AMOUNT)", olTotal.getAmount().toString()});
            writer.writeNext(new String[]{"sum(OL_QUANTITY)", olTotal.getQuantity().toString()});
            writer.writeNext(new String[]{"sum(S_QUANTITY)", stockTotal.getQuantity().toString()});
            writer.writeNext(new String[]{"sum(S_YTD)", stockTotal.getYtdQuantity().toString()});
            writer.writeNext(new String[]{"sum(S_ORDER_CNT)", stockTotal.getOrderCount().toString()});
            writer.writeNext(new String[]{"sum(S_REMOTE_CNT)", stockTotal.getRemoteOrderCount().toString()});
            writer.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
