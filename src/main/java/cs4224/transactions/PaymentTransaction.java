package cs4224.transactions;

import com.datastax.oss.driver.api.core.CqlSession;
import cs4224.ParallelExecutor;
import cs4224.dao.CustomerDao;
import cs4224.dao.DistrictDao;
import cs4224.dao.WarehouseDao;
import cs4224.entities.Customer;
import cs4224.entities.District;
import cs4224.entities.Warehouse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static cs4224.utils.Constants.MAX_RETRIES;

public class PaymentTransaction extends BaseTransaction {
    private static final Logger LOG = LoggerFactory.getLogger(PaymentTransaction.class);
    private final ExecutorService executorService;
    private final WarehouseDao warehouseDao;
    private final DistrictDao districtDao;
    private final CustomerDao customerDao;

    public PaymentTransaction(CqlSession session, ExecutorService executorService,
                              WarehouseDao warehouseDao, DistrictDao districtDao, CustomerDao customerDao) {
        super(session);
        this.executorService = executorService;
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

    @Override
    public String getType() {
        return "Payment";
    }

    private List<Object> getEntities(final int customerWarehouseId, final int customerDistrictId,
                                     final int customerId) {
        final ParallelExecutor getEntitiesExecutor = new ParallelExecutor(executorService)
                .addTask(() -> Warehouse.map(warehouseDao.getById(customerWarehouseId)))
                .addTask(() -> District.map(districtDao.getById(customerWarehouseId, customerDistrictId)))
                .addTask(() -> Customer.map(customerDao.getById(customerWarehouseId, customerDistrictId, customerId)));
        return getEntitiesExecutor.execute();
    }

    private List<Object> updateEntities(final List<Object> entities, final int customerWarehouseId,
                                        final int customerDistrictId, final int customerId, final double paymentAmount) {
        final ParallelExecutor parallelExecutor = new ParallelExecutor(executorService)
                .addTask(() -> updateWarehouse((Warehouse) entities.get(0), customerWarehouseId, customerDistrictId))
                .addTask(() -> updateDistrict((District) entities.get(1), customerWarehouseId, customerDistrictId,
                        paymentAmount))
                .addTask(() -> updateCustomer((Customer) entities.get(2), customerWarehouseId, customerDistrictId,
                        customerId, paymentAmount));
        return parallelExecutor.execute();
    }

    private Warehouse updateWarehouse(Warehouse warehouse, final int customerWarehouseId, final double paymentAmount) {
        final Warehouse updatedWarehouse = new Warehouse();
        Boolean isApplied = false;
        int numRetries = 0;
        while (numRetries < MAX_RETRIES && !isApplied) {
            try {
                updatedWarehouse.setAmountPaidYTD(warehouse.getAmountPaidYTD().add(new BigDecimal(paymentAmount)));
                isApplied = warehouseDao.updateWhereIdEquals(updatedWarehouse, customerWarehouseId,
                        warehouse.getAmountPaidYTD());
            } catch (Exception ignored) { }
            if (!isApplied) {
                warehouse = Warehouse.map(warehouseDao.getById(customerWarehouseId));
                numRetries++;
            }
        }
        if (numRetries == MAX_RETRIES) {
            LOG.error("Error while updating warehouse");
            throw new RuntimeException("Error while updating customer");
        }
        warehouse.setAmountPaidYTD(updatedWarehouse.getAmountPaidYTD());
        return warehouse;
    }

    private District updateDistrict(District district, final int customerWarehouseId, final int customerDistrictId,
                                    final double paymentAmount) {
        final District updatedDistrict = new District();
        Boolean isApplied = false;
        int numRetries = 0;
        while (numRetries < MAX_RETRIES && !isApplied) {
            try {
                updatedDistrict.setAmountPaidYTD(district.getAmountPaidYTD().add(new BigDecimal(paymentAmount)));
                isApplied = districtDao.updateWhereIdEquals(updatedDistrict, customerWarehouseId, customerDistrictId,
                        district.getAmountPaidYTD());
            } catch (Exception ignored) { }
            if (!isApplied) {
                district = District.map(districtDao.getById(customerWarehouseId, customerDistrictId));
                numRetries++;
            }
        }
        if (numRetries == MAX_RETRIES) {
            LOG.error("Error while updating district");
            throw new RuntimeException("Error while updating customer");
        }
        district.setAmountPaidYTD(updatedDistrict.getAmountPaidYTD());
        return district;
    }

    private Customer updateCustomer(Customer customer, final int customerWarehouseId, final int customerDistrictId,
                                    final int customerId, final double paymentAmount) {
        final Customer updatedCustomer = new Customer();
        int numRetries = 0;
        Boolean isApplied = false;
        while (numRetries < MAX_RETRIES && !isApplied) {
            try {
                updatedCustomer.setBalance(customer.getBalance().subtract(new BigDecimal(paymentAmount)))
                        .setPaymentYTD((float) (customer.getPaymentYTD() + paymentAmount))
                        .setNumPayments(customer.getNumPayments() + 1);
                isApplied = customerDao.updateWhereIdEquals(updatedCustomer, customerWarehouseId, customerDistrictId,
                        customerId, customer.getPaymentYTD());
            } catch (Exception ignored) {
            }
            if (!isApplied) {
                customer = Customer.map(customerDao.getById(customerWarehouseId, customerDistrictId, customerId));
                numRetries++;
            }
        }
        if (numRetries == MAX_RETRIES) {
            LOG.error("Error while updating customer");
            throw new RuntimeException("Error while updating customer");
        }
        customer.setBalance(updatedCustomer.getBalance()).setPaymentYTD(updatedCustomer.getPaymentYTD())
                .setNumPayments(updatedCustomer.getNumPayments());
        return customer;
    }

    private void printOutput(final Warehouse warehouse, final District district, final Customer customer,
                             final double paymentAmount) {
        System.out.printf("\n Customer Identifier (C_W_ID, C_D_ID, C_ID): %s", customer.toSpecifier());
        System.out.println(customer.toName());
        System.out.println(customer.toAddress());
        System.out.println(customer.toOtherInfo());
        System.out.println(warehouse.toAddress());
        System.out.println(district.toAddress());
        System.out.printf(" Payment Amount: %f", paymentAmount);
    }
}
