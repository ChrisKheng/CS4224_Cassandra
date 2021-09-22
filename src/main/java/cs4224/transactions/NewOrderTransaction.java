package cs4224.transactions;

import com.datastax.oss.driver.api.core.CqlSession;

public class NewOrderTransaction extends BaseTransaction {
    private final int customerId;
    private final int warehouseId;
    private final int districtId;
    private final int noOfItems;

    public NewOrderTransaction(CqlSession session, String[] parameters) {
        super(session, parameters);
        customerId = Integer.parseInt(parameters[1]);
        warehouseId = Integer.parseInt(parameters[2]);
        districtId = Integer.parseInt(parameters[3]);
        noOfItems = Integer.parseInt(parameters[4]);
    }

    @Override
    public int getExtraLines() {
        return noOfItems;
    }

    @Override
    public void execute(String[] dataLines) {
        System.out.printf("Running New Order Transaction with C_ID= %d, W_ID=%d, D_ID=%d, N=%d \n", customerId, warehouseId, districtId, noOfItems);
        for (String line : dataLines) {
            System.out.println(line);
        }
    }
}
