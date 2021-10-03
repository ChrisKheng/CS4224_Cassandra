package cs4224.dao;

import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.Query;
import cs4224.entities.order.OrderPopularItem;

import java.util.stream.Stream;

@Dao
public interface OrderDao {

    @Query("SELECT O_ID, O_ENTRY_D, O_C_ID FROM ${qualifiedTableId} WHERE O_W_ID = :o_w_id AND O_D_ID = :o_d_id " +
            "AND O_ID >= :gt_id AND O_ID < :lt_id")
    Stream<OrderPopularItem> getById(@CqlName("o_w_id") int warehouseId, @CqlName("o_d_id") int districtId,
                                     @CqlName("gt_id") int greaterThanId, @CqlName("lt_id") int lessThanId);
}
