package cs4224.transactions;

import com.datastax.oss.driver.api.core.CqlSession;
import cs4224.ParallelExecutor;
import cs4224.dao.CustomerDao;
import cs4224.dao.DistrictDao;
import cs4224.dao.WarehouseDao;
import cs4224.entities.Customer;
import cs4224.entities.District;
import cs4224.entities.Warehouse;

import java.math.BigDecimal;
import java.util.List;

public class PaymentTransaction extends BaseTransaction {
    private final WarehouseDao warehouseDao;
    private final DistrictDao districtDao;
    private final CustomerDao customerDao;

    public PaymentTransaction(CqlSession session, WarehouseDao warehouseDao,
                              DistrictDao districtDao, CustomerDao customerDao) {
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

        List<Object> entities = getEntities(customerWarehouseId, customerDistrictId, customerId);
        List<Object> updatedEntities = updateEntities(entities, customerWarehouseId, customerDistrictId, customerId,
                paymentAmount);
        printOutput((Warehouse) updatedEntities.get(0), (District) updatedEntities.get(1),
                (Customer) updatedEntities.get(2), paymentAmount);
    }

    private List<Object> getEntities(final int customerWarehouseId, final int customerDistrictId,
                                     final int customerId) {
        final ParallelExecutor getEntitiesExecutor = new ParallelExecutor()
                .addTask(() -> Warehouse.map(warehouseDao.getById(customerWarehouseId)))
                .addTask(() -> District.map(districtDao.getById(customerWarehouseId, customerDistrictId)))
                .addTask(() -> Customer.map(customerDao.getById(customerWarehouseId, customerDistrictId, customerId)));
        return getEntitiesExecutor.execute();
    }

    private List<Object> updateEntities(final List<Object> entities, final int customerWarehouseId,
                                        final int customerDistrictId, final int customerId, final double paymentAmount) {
        final ParallelExecutor parallelExecutor = new ParallelExecutor()
                .addTask(() -> updateWarehouse((Warehouse) entities.get(0), customerWarehouseId, customerDistrictId))
                .addTask(() -> updateDistrict((District) entities.get(1), customerWarehouseId, customerDistrictId,
                        paymentAmount))
                .addTask(() -> updateCustomer((Customer) entities.get(2), customerWarehouseId, customerDistrictId,
                        customerId, paymentAmount));
        return parallelExecutor.execute();
    }

    private Warehouse updateWarehouse(final Warehouse warehouse, final int customerWarehouseId, final double paymentAmount) {
        final Warehouse updatedWarehouse = new Warehouse();
        updatedWarehouse.setAmountPaidYTD(warehouse.getAmountPaidYTD().add(new BigDecimal(paymentAmount)));
        warehouseDao.updateWhereIdEquals(updatedWarehouse, customerWarehouseId, warehouse.getAmountPaidYTD());
        return warehouse;
    }

    private District updateDistrict(final District district, final int customerWarehouseId, final int customerDistrictId,
                                    final double paymentAmount) {
        final District updatedDistrict = new District();
        updatedDistrict.setAmountPaidYTD(district.getAmountPaidYTD().add(new BigDecimal(paymentAmount)));
        districtDao.updateWhereIdEquals(updatedDistrict, customerWarehouseId, customerDistrictId,
                district.getAmountPaidYTD());
        return district;
    }

    private Customer updateCustomer(final Customer customer, final int customerWarehouseId, final int customerDistrictId,
                                    final int customerId, final double paymentAmount) {
        final Customer updatedCustomer = new Customer();
        updatedCustomer.setBalance(customer.getBalance().subtract(new BigDecimal(paymentAmount)));
        updatedCustomer.setPaymentYTD((float) (customer.getPaymentYTD() - paymentAmount));
        updatedCustomer.setNumPayments(customer.getNumPayments() + 1);
        customerDao.updateWhereIdEquals(updatedCustomer, customerWarehouseId, customerDistrictId,
                customerId, customer.getPaymentYTD());
        return customer;
    }

    private void printOutput(final Warehouse warehouse, final District district, final Customer customer,
                             final double paymentAmount) {
        System.out.printf(" %s\n", customer);
        System.out.printf(" Warehouse(%s)\n", warehouse.addressToString());
        System.out.printf(" District(%s)\n", district.addressToString());
        System.out.printf(" Payment Amount= %f\n", paymentAmount);
    }
}
