package cs4224.models;

import java.util.Objects;

public class Customer {
    protected final int customerWarehouseId;
    protected final int customerDistrictId;
    protected final int customerId;

    public Customer(int customerWarehouseId, int customerDistrictId, int customerId) {
        this.customerWarehouseId = customerWarehouseId;
        this.customerDistrictId = customerDistrictId;
        this.customerId = customerId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Customer customer = (Customer) o;
        return customerWarehouseId == customer.customerWarehouseId && customerDistrictId == customer.customerDistrictId && customerId == customer.customerId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(customerWarehouseId, customerDistrictId, customerId);
    }

    @Override
    public String toString() {
        return String.format("(%d, %d, %d)", customerWarehouseId, customerDistrictId, customerId);
    }
}