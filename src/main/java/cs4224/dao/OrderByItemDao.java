package cs4224.dao;

import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.Query;

import java.util.List;

@Dao
public interface OrderByItemDao {

    @Query("SELECT count(*) FROM ${qualifiedTableId} WHERE O_W_ID = :o_w_id AND O_D_ID = :o_d_id " +
            "AND I_ID = :i_id AND O_ID IN :o_ids")
    long getCountByItemId(@CqlName("o_w_id") int warehouseId, @CqlName("o_d_id") int districtId,
                          @CqlName("i_id") int itemId, @CqlName("o_ids") List<Integer> orderIds);
}
