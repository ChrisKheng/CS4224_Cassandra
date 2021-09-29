package cs4224.transactions;

import com.datastax.oss.driver.api.core.CqlSession;

public class OrderStatusTransaction extends BaseTransaction {

    public OrderStatusTransaction(final CqlSession session) {
        super(session);


    }

    @Override
    public void execute(String[] dataLines,  String[] parameters) {
        final int warehouseId = Integer.parseInt(parameters[1]);
        final int districtId = Integer.parseInt(parameters[2]);
        final int customerId = Integer.parseInt(parameters[3]);
    }
}
