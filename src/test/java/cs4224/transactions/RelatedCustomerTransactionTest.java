package cs4224.transactions;

import cs4224.extensions.InitializationExtension;
import cs4224.models.Customer;
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
        RelatedCustomerTransaction transaction = new RelatedCustomerTransaction(InitializationExtension.session,
                new String[]{"R", "1", "1", "1"});
        HashSet<Customer> relatedCustomers = transaction.executeAndGetResult();
        HashSet<Customer> expectedResult = new HashSet<>(Arrays.asList(
                new Customer(2, 2, 2),
                new Customer(5, 3, 3))
        );

        Assertions.assertEquals(expectedResult, relatedCustomers);
    }
}
