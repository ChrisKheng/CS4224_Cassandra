package cs4224.models;

import java.util.Objects;

public class Order {
    public final int orderWarehouseId;
    public final int orderDistrictId;
    public final int orderId;

    public Order(int orderWarehouseId, int orderDistrictId, int orderId) {
        this.orderWarehouseId = orderWarehouseId;
        this.orderDistrictId = orderDistrictId;
        this.orderId = orderId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Order order = (Order) o;
        return orderWarehouseId == order.orderWarehouseId && orderDistrictId == order.orderDistrictId && orderId == order.orderId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(orderWarehouseId, orderDistrictId, orderId);
    }

    @Override
    public String toString() {
        return String.format("(%d, %d, %d)", orderWarehouseId, orderDistrictId, orderId);
    }
}
