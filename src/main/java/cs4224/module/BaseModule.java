package cs4224.module;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.session.SessionBuilder;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import cs4224.dao.*;
import cs4224.mapper.*;
import cs4224.transactions.*;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static cs4224.utils.Constants.*;

public class BaseModule extends AbstractModule {
    private final String keyspace;
    private final String ip;
    private final int port;

    public BaseModule(String keyspace, String ip, int port) {
        this.keyspace = keyspace;
        this.ip = ip;
        this.port = port;
    }

    @Override
    protected void configure() {
    }


    @Provides
    @Singleton
    public CqlSession provideCqlSession() {
        final SessionBuilder rawSession = CqlSession.builder().withKeyspace(CqlIdentifier.fromCql(keyspace));

        if (ip.isEmpty()) {
            return ((CqlSessionBuilder) rawSession).build();
        } else {
            int actualPort = port == -1 ? CASSANDRA_PORT : port;
            return ((CqlSessionBuilder) rawSession)
                    .addContactPoint(new InetSocketAddress(ip, actualPort))
                    .withLocalDatacenter("dc1")
                    .build();
        }
    }

    @Provides
    @Inject
    public WarehouseDao provideWarehouseDao(CqlSession session) {
        return new WarehouseMapperBuilder(session).build().dao(WAREHOUSE_TABLE);
    }

    @Provides
    @Inject
    public DistrictDao provideDistrictDao(CqlSession session) {
        return new DistrictMapperBuilder(session).build().dao(DISTRICT_TABLE);
    }

    @Provides
    @Inject
    public CustomerDao provideCustomerDao(CqlSession session) {
        return new CustomerMapperBuilder(session).build().dao(CUSTOMER_TABLE);
    }

    @Provides
    @Inject
    public OrderDao provideOrderDao(CqlSession session) {
        return new OrderMapperBuilder(session).build().dao(ORDER_TABLE);
    }

    @Provides
    @Inject
    public OrderLineDao provideOrderLineDao(CqlSession session) {
        return new OrderLineMapperBuilder(session).build().dao(ORDER_LINE_TABLE);
    }

    @Provides
    @Inject
    public ItemDao provideItemDao(CqlSession session) {
        return new ItemMapperBuilder(session).build().dao(ITEM_TABLE);
    }

    @Provides
    @Inject
    public OrderByItemDao provideOrderByItemDao(CqlSession session) {
        return new OrderItemMapperBuilder(session).build().dao(ORDER_BY_ITEM_TABLE);
    }

    @Provides
    @Inject
    public StockDao provideStockDao(CqlSession session) {
        return new StockMapperBuilder(session).build().dao(STOCK_TABLE);
    }

    @Provides
    @Singleton
    public ExecutorService provideExecutorService() {
        return Executors.newFixedThreadPool(5);
    }

    @Provides
    @Inject
    public PaymentTransaction providePaymentTransaction(CqlSession session, ExecutorService executorService,
                                                        WarehouseDao warehouseDao, DistrictDao districtDao,
                                                        CustomerDao customerDao) {
        return new PaymentTransaction(session, executorService, warehouseDao, districtDao, customerDao);
    }

    @Provides
    @Inject
    public NewOrderTransaction provideNewOrderTransaction(CqlSession session) {
        System.out.println(session.getContext().getConfigLoader().getInitialConfig()
                .getDefaultProfile().getDuration(DefaultDriverOption.REQUEST_TIMEOUT));

        return new NewOrderTransaction(session);
    }

    @Provides
    @Inject
    public DeliveryTransaction provideDeliveryTransaction(CqlSession session) {
        return new DeliveryTransaction(session);
    }

    @Provides
    @Inject
    public OrderStatusTransaction provideOrderStatusTransaction(CqlSession session) {
        return new OrderStatusTransaction(session);
    }

    @Provides
    @Inject
    public PopularItemTransaction providePopularItemTransaction(CqlSession session, DistrictDao districtDao,
                                                                CustomerDao customerDao, OrderDao orderDao,
                                                                OrderLineDao orderLineDao, ItemDao itemDao,
                                                                OrderByItemDao orderByItemDao) {
        return new PopularItemTransaction(session, districtDao, customerDao, orderDao, orderLineDao, itemDao,
                orderByItemDao);
    }

    @Provides
    @Inject
    public RelatedCustomerTransaction provideRelatedCustomerTransaction(CqlSession session, ExecutorService executorService) {
        return new RelatedCustomerTransaction(session);
    }

    @Provides
    @Inject
    public TopBalanceTransaction provideTopBalanceTransaction(CqlSession session, ExecutorService executorService) {
        return new TopBalanceTransaction(session, executorService);
    }

    @Provides
    @Inject
    public StockLevelTransaction provideStockLevelTransaction(CqlSession session) {
        return new StockLevelTransaction(session);
    }
}
