package cs4224.transactions;

import com.datastax.oss.driver.api.core.CqlSession;

public class RelatedCustomerTransaction extends BaseTransaction {
    private final int customerWarehouseId;
    private final int customerDistrictId;
    private final int customerId;

    public RelatedCustomerTransaction(CqlSession session, String[] parameters) {
        super(session, parameters);
        customerWarehouseId = Integer.parseInt(parameters[1]);
        customerDistrictId = Integer.parseInt(parameters[2]);
        customerId = Integer.parseInt(parameters[3]);
    }

    @Override
    public void execute(String[] dataLines) {

    }
}
