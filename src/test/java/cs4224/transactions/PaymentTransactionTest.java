package cs4224.transactions;

import com.datastax.oss.driver.api.core.CqlSession;
import cs4224.dao.CustomerDao;
import cs4224.dao.DistrictDao;
import cs4224.dao.WarehouseDao;
import cs4224.extensions.InitializationExtension;
import cs4224.mapper.CustomerMapperBuilder;
import cs4224.mapper.DistrictMapperBuilder;
import cs4224.mapper.WarehouseMapperBuilder;
import cs4224.utils.Utils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static cs4224.utils.Constants.*;

@ExtendWith({InitializationExtension.class})
public class PaymentTransactionTest {

    @BeforeAll
    public static void setup() {
        Utils.executeBashCommand("cqlsh -f src/test/resources/test_data/related_customer_transaction/load_data.cql");
    }

    @Test
    public void testExecutePaymentTransaction() {
        final CqlSession session = InitializationExtension.session;
        final WarehouseDao warehouseDao = new WarehouseMapperBuilder(session).build().dao(WAREHOUSE_TABLE);
        final DistrictDao districtDao = new DistrictMapperBuilder(session).build().dao(DISTRICT_TABLE);;
        final CustomerDao customerDao = new CustomerMapperBuilder(session).build().dao(CUSTOMER_TABLE);


        PaymentTransaction transaction = new PaymentTransaction(session, warehouseDao, districtDao, customerDao);
        transaction.execute(new String[0], new String[]{"P","8","1","1267","122.34"});
    }
}

