package cs4224.transactions;

import com.datastax.oss.driver.api.core.CqlSession;

import java.util.Arrays;

public class DeliveryTransaction extends BaseTransaction{


    private static final int NO_OF_DISTRICTS = 10;
    public DeliveryTransaction(CqlSession session) {
        super(session);
    }

    @Override
    public void execute(String[] dataLines, String[] parameters) {
        final int warehouseId = Integer.parseInt(parameters[1]);
        final int carrierId = Integer.parseInt(parameters[2]);
        /*
            Given W_ID, CARRIER_ID
            For DISTRICT_NO from 1 to 10:
                a Find the oldest yet-to-be-delivered order -> order X , customer C
                b Update the order's carrier info in X
                c Update all the order-lines in X
                d Update customer C:
                    Increment C_BALANCE
                    Increment C_DELIVERY_CNT


         */

        for (int districtNo = 1; districtNo <= NO_OF_DISTRICTS; districtNo++){







        }

        System.out.printf("Running Delivery Transaction with W_ID=%d , CAREER_ID= %d \n", warehouseId, carrierId);
        Arrays.stream(dataLines).forEach(System.out::println);
    }
}
