package cs4224.transactions;

import cs4224.extensions.InitializationExtension;
import cs4224.utils.Utils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith({InitializationExtension.class})
public class PaymentTransactionTest {

    @BeforeAll
    public static void setup() {
        Utils.executeBashCommand("cqlsh -f src/test/resources/test_data/related_customer_transaction/load_data.cql");
    }

    @Test
    public void testExecutePaymentTransaction() {
        PaymentTransaction transaction = new PaymentTransaction(InitializationExtension.session,
                new String[]{"P","8","1","1267","122.34"});
        transaction.execute(new String[0]);
    }
}

