package cs4224.module;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.session.SessionBuilder;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import cs4224.dao.CustomerDao;
import cs4224.dao.DistrictDao;
import cs4224.dao.WarehouseDao;
import cs4224.mapper.CustomerMapperBuilder;
import cs4224.mapper.DistrictMapperBuilder;
import cs4224.mapper.WarehouseMapperBuilder;
import cs4224.transactions.*;

import java.net.InetSocketAddress;

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
        SessionBuilder rawSession = CqlSession.builder().withKeyspace(CqlIdentifier.fromCql(keyspace));

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
    public PaymentTransaction providePaymentTransaction(CqlSession session, WarehouseDao warehouseDao,
                                                         DistrictDao districtDao, CustomerDao customerDao) {
        return new PaymentTransaction(session, warehouseDao, districtDao, customerDao);
    }

    @Provides
    @Inject
    public NewOrderTransaction provideNewOrderTransaction(CqlSession session) {
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
    public PopularItemTransaction providePopularItemTransaction(CqlSession session) {
        return new PopularItemTransaction(session);
    }

    @Provides
    @Inject
    public RelatedCustomerTransaction provideRelatedCustomerTransaction(CqlSession session) {
        return new RelatedCustomerTransaction(session);
    }

    @Provides
    @Inject
    public TopBalanceTransaction provideTopBalanceTransaction(CqlSession session) {
        return new TopBalanceTransaction(session);
    }

    @Provides
    @Inject
    public StockLevelTransaction provideStockLevelTransaction(CqlSession session) {
        return new StockLevelTransaction(session);
    }
}
