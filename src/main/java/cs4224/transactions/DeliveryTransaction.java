package cs4224.transactions;

import com.datastax.oss.driver.api.core.CqlSession;

import java.util.Arrays;

public class DeliveryTransaction extends BaseTransaction{
    private final int warehouseId;
    private final int carrierId;

    public DeliveryTransaction(CqlSession session, String[] parameters) {
        super(session, parameters);
        warehouseId = Integer.parseInt(parameters[1]);
        carrierId = Integer.parseInt(parameters[2]);
    }

    @Override
    public void execute(String[] dataLines) {
        System.out.printf("Running Delivery Transaction with W_ID=%d , CAREER_ID= %d \n", warehouseId, carrierId);
        Arrays.stream(dataLines).forEach(System.out::println);
    }
}
