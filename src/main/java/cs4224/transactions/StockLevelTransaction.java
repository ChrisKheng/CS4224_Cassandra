package cs4224.transactions;

import com.datastax.oss.driver.api.core.CqlSession;

public class StockLevelTransaction extends BaseTransaction {
    private final int warehouseId;
    private final int districtId;
    private final int threshold;
    private final int noOfOrders;

    public StockLevelTransaction(CqlSession session, String[] parameters) {
        super(session, parameters);
        warehouseId = Integer.parseInt(parameters[1]);
        districtId = Integer.parseInt(parameters[2]);
        threshold = Integer.parseInt(parameters[3]);
        noOfOrders = Integer.parseInt(parameters[4]);
    }

    @Override
    public void execute(String[] dataLines) {

    }
}
