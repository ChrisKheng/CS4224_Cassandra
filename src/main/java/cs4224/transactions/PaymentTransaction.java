package cs4224.transactions;

import com.datastax.oss.driver.api.core.CqlSession;
import cs4224.entities.Customer;
import cs4224.entities.District;
import cs4224.entities.Warehouse;
import cs4224.mapper.*;

import java.math.BigDecimal;

public class PaymentTransaction extends BaseTransaction {
    private final static String WAREHOUSE_TABLE = "WAREHOUSE";
    private final static String DISTRICT_TABLE = "DISTRICT";
    private final static String CUSTOMER_TABLE = "CUSTOMER";


    private final int customerWarehouseId;
    private final int customerDistrictId;
    private final int customerId;
    private final double paymentAmount;
    private final WarehouseMapper warehouseMapper;
    private final DistrictMapper districtMapper;
    private final CustomerMapper customerMapper;

    public PaymentTransaction(CqlSession session, String[] parameters) {
        super(session, parameters);
        customerWarehouseId = Integer.parseInt(parameters[1]);
        customerDistrictId = Integer.parseInt(parameters[2]);
        customerId = Integer.parseInt(parameters[3]);
        paymentAmount = Double.parseDouble(parameters[4]);
        warehouseMapper = new WarehouseMapperBuilder(session).build();
        districtMapper = new DistrictMapperBuilder(session).build();
        customerMapper = new CustomerMapperBuilder(session).build();
    }

    @Override
    public void execute(String[] dataLines) {
        final Warehouse warehouse = warehouseMapper.dao(WAREHOUSE_TABLE).getById(customerWarehouseId);
        final Warehouse updatedWarehouse = new Warehouse();
        updatedWarehouse.setAmountPaidYTD(warehouse.getAmountPaidYTD().add(new BigDecimal(paymentAmount)));
        warehouseMapper.dao(WAREHOUSE_TABLE).updateWhereIdEquals(updatedWarehouse, customerWarehouseId);

        final District district = districtMapper.dao(DISTRICT_TABLE).getById(customerWarehouseId, customerDistrictId);
        final District updatedDistrict = new District();
        updatedDistrict.setAmountPaidYTD(district.getAmountPaidYTD().add(new BigDecimal(paymentAmount)));
        districtMapper.dao(DISTRICT_TABLE).updateWhereIdEquals(updatedDistrict, customerWarehouseId, customerDistrictId);

        final Customer customer = customerMapper.dao(CUSTOMER_TABLE).getById(customerWarehouseId, customerDistrictId,
                customerId);
        final Customer updatedCustomer = new Customer();
        updatedCustomer.setBalance(customer.getBalance().subtract(new BigDecimal(paymentAmount)));
        updatedCustomer.setPaymentYTD((float) (customer.getPaymentYTD() - paymentAmount));
        updatedCustomer.setNumPayments(customer.getNumPayments() + 1);
        customerMapper.dao(CUSTOMER_TABLE).updateWhereIdEquals(updatedCustomer, customerWarehouseId, customerDistrictId,
                customerId);

    }
}
