package cs4224.transactions;

import cs4224.extensions.InitializationExtension;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith({InitializationExtension.class})
public class RelatedCustomerTransactionTest {
    @Test
    public void testExecuteHasRelatedCustomer() {
        Assertions.assertEquals(1, 1);
    }
}
