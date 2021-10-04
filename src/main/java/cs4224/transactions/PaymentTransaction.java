package cs4224.transactions;

import com.datastax.oss.driver.api.core.CqlSession;
import cs4224.dao.CustomerDao;
import cs4224.dao.DistrictDao;
import cs4224.dao.WarehouseDao;
import cs4224.entities.Customer;
import cs4224.entities.District;
import cs4224.entities.Warehouse;

import java.math.BigDecimal;

public class PaymentTransaction extends BaseTransaction {
    private final WarehouseDao warehouseDao;
    private final DistrictDao districtDao;
    private final CustomerDao customerDao;

    public PaymentTransaction(CqlSession session, WarehouseDao warehouseDao, DistrictDao districtDao,
                              CustomerDao customerDao) {
        super(session);
        this.warehouseDao = warehouseDao;
        this.districtDao = districtDao;
        this.customerDao = customerDao;
    }

    @Override
    public void execute(String[] dataLines, String[] parameters) {
        final int customerWarehouseId = Integer.parseInt(parameters[1]);
        final int customerDistrictId = Integer.parseInt(parameters[2]);
        final int customerId = Integer.parseInt(parameters[3]);
        final double paymentAmount = Double.parseDouble(parameters[4]);

        final Warehouse warehouse = Warehouse.map(warehouseDao.getById(customerWarehouseId));
        final Warehouse updatedWarehouse = new Warehouse();
        updatedWarehouse.setAmountPaidYTD(warehouse.getAmountPaidYTD().add(new BigDecimal(paymentAmount)));
        warehouseDao.updateWhereIdEquals(updatedWarehouse, customerWarehouseId);

        final District district = District.map(districtDao.getById(customerWarehouseId, customerDistrictId));
        final District updatedDistrict = new District();
        updatedDistrict.setAmountPaidYTD(district.getAmountPaidYTD().add(new BigDecimal(paymentAmount)));
        districtDao.updateWhereIdEquals(updatedDistrict, customerWarehouseId, customerDistrictId);

        final Customer customer = Customer.map(customerDao.getById(customerWarehouseId, customerDistrictId,
                customerId));
        final Customer updatedCustomer = new Customer();
        updatedCustomer.setBalance(customer.getBalance().subtract(new BigDecimal(paymentAmount)));
        updatedCustomer.setPaymentYTD((float) (customer.getPaymentYTD() - paymentAmount));
        updatedCustomer.setNumPayments(customer.getNumPayments() + 1);
        customerDao.updateWhereIdEquals(updatedCustomer, customerWarehouseId, customerDistrictId,
                customerId);

        printOutput(customer, warehouse, district, paymentAmount);
    }

    private void printOutput(final Customer customer, final Warehouse warehouse, final District district,
                             final double paymentAmount ) {
        System.out.printf(" %s\n", customer);
        System.out.printf(" Warehouse(%s)\n", warehouse.addressToString());
        System.out.printf(" District(%s)\n", district.addressToString());
        System.out.printf(" Payment Amount= %f\n", paymentAmount);
    }
}
