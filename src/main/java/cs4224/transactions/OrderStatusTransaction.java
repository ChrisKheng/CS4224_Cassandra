package cs4224.transactions;

import com.datastax.oss.driver.api.core.CqlSession;

public class OrderStatusTransaction extends BaseTransaction {
    private final int warehouseId;
    private final int districtId;
    private final int customerId;

    public OrderStatusTransaction(final CqlSession session, final String[] parameters) {
        super(session, parameters);

        warehouseId = Integer.parseInt(parameters[1]);
        districtId = Integer.parseInt(parameters[2]);
        customerId = Integer.parseInt(parameters[3]);
    }

    @Override
    public void execute(String[] dataLines) {

    }
}
