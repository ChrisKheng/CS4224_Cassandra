package cs4224.transactions;

import com.datastax.oss.driver.api.core.CqlSession;

public class StockLevelTransaction extends BaseTransaction {

    public StockLevelTransaction(CqlSession session, String[] parameters) {
        super(session);

    }

    @Override
    public void execute(String[] dataLines, String[] parameters) {
        final int warehouseId = Integer.parseInt(parameters[1]);
        final int districtId = Integer.parseInt(parameters[2]);
        final int threshold = Integer.parseInt(parameters[3]);
        final int noOfOrders = Integer.parseInt(parameters[4]);
    }
}
