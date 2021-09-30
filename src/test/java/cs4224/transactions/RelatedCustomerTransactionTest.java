package cs4224.transactions;

import cs4224.extensions.InitializationExtension;
import cs4224.entities.customer.Customer;
import cs4224.utils.Utils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.HashSet;

@ExtendWith({InitializationExtension.class})
public class RelatedCustomerTransactionTest {
    @BeforeAll
    public static void setup() {
        Utils.executeBashCommand("cqlsh -f src/test/resources/test_data/related_customer_transaction/load_data.cql");
    }

    @Test
    public void testExecuteHasRelatedCustomer() {
        RelatedCustomerTransaction transaction = new RelatedCustomerTransaction(InitializationExtension.session);
        HashSet<Customer> relatedCustomers = transaction.executeAndGetResult(1, 1, 1);
        HashSet<Customer> expectedResult = new HashSet<>(Arrays.asList(
                Customer.builder().warehouseId(2).districtId(2).id(2).build(),
                Customer.builder().warehouseId(5).districtId(3).id(3).build()
        ));

        Assertions.assertEquals(expectedResult, relatedCustomers);
    }
}
