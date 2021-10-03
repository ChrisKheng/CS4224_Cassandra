package cs4224.dao;

import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.Query;
import cs4224.entities.orderline.OrderLineItem;
import cs4224.entities.orderline.OrderLineQuantity;

import java.math.BigDecimal;
import java.util.stream.Stream;

@Dao
public interface OrderLineDao {

    @Query("SELECT max(OL_QUANTITY) as OL_QUANTITY FROM ${qualifiedTableId} WHERE OL_W_ID = :ol_w_id AND OL_D_ID = :ol_d_id " +
            "AND OL_O_ID = :ol_o_id")
    OrderLineQuantity getOLQuantity(@CqlName("ol_w_id") int warehouseId, @CqlName("ol_d_id") int districtId,
                                    @CqlName("ol_o_id") int orderId);

    @Query("SELECT OL_I_ID FROM ${qualifiedTableId} WHERE OL_W_ID = :ol_w_id AND OL_D_ID = :ol_d_id " +
            "AND OL_O_ID = :ol_o_id AND OL_QUANTITY = :ol_quantity ALLOW FILTERING")
    Stream<OrderLineItem> getOLItemId(@CqlName("ol_w_id") int warehouseId, @CqlName("ol_d_id") int districtId,
                                      @CqlName("ol_o_id") int orderId, @CqlName("ol_quantity") BigDecimal orderLineQuantity);
}
