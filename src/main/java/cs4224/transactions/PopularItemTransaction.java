package cs4224.transactions;

import com.datastax.oss.driver.api.core.CqlSession;

public class PopularItemTransaction extends BaseTransaction {
    private final int warehouseId;
    private final int districtId;
    private final int L;

    public PopularItemTransaction(CqlSession session, String[] parameters) {
        super(session, parameters);
        warehouseId = Integer.parseInt(parameters[1]);
        districtId = Integer.parseInt(parameters[2]);
        L = Integer.parseInt(parameters[3]);
    }

    @Override
    public void execute(String[] dataLines) {

    }
}
