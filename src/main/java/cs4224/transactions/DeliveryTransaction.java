package cs4224.transactions;

import com.datastax.oss.driver.api.core.CqlSession;

import java.util.Arrays;

public class DeliveryTransaction extends BaseTransaction{

    public DeliveryTransaction(CqlSession session) {
        super(session);

    }

    @Override
    public void execute(String[] dataLines, String[] parameters) {
        final int warehouseId = Integer.parseInt(parameters[1]);
        final int carrierId = Integer.parseInt(parameters[2]);


        System.out.printf("Running Delivery Transaction with W_ID=%d , CAREER_ID= %d \n", warehouseId, carrierId);
        Arrays.stream(dataLines).forEach(System.out::println);
    }
}
