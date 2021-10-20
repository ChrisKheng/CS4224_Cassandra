package cs4224.transactions;

import com.datastax.oss.driver.api.core.CqlSession;
import cs4224.dao.*;
import cs4224.extensions.InitializationExtension;
import cs4224.mapper.*;
import cs4224.utils.Utils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static cs4224.utils.Constants.*;

@ExtendWith({InitializationExtension.class})
public class PopularItemTransactionTest {

    @BeforeAll
    public static void setup() {
        Utils.executeBashCommand("cqlsh -f src/test/resources/test_data/related_customer_transaction/load_data.cql");
    }

    @Test
    public void testExecutePopularItemTransaction() {
        final CqlSession session = InitializationExtension.session;
        final DistrictDao districtDao = new DistrictMapperBuilder(session).build().dao(DISTRICT_TABLE);
        final CustomerDao customerDao = new CustomerMapperBuilder(session).build().dao(CUSTOMER_TABLE);
        final OrderDao orderDao = new OrderMapperBuilder(session).build().dao(ORDER_TABLE);
        final OrderLineDao orderLineDao = new OrderLineMapperBuilder(session).build().dao(ORDER_LINE_TABLE);
        final ItemDao itemDao = new ItemMapperBuilder(session).build().dao(ITEM_TABLE);

        PopularItemTransaction transaction = new PopularItemTransaction(session, districtDao, customerDao, orderDao,
                orderLineDao, itemDao);
        transaction.execute(new String[0], new String[]{"I","8","1","27"});
    }
}

