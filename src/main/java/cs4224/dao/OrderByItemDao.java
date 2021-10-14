package cs4224.dao;

import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.Query;

import java.util.List;

@Dao
public interface OrderByItemDao {

    @Query("SELECT count(*) FROM ${qualifiedTableId} WHERE O_W_ID = :warehouseId AND O_D_ID = :districtId " +
            "AND I_ID = :itemId AND O_ID IN :orderIds")
    long getCountByItemId(int warehouseId, int districtId, int itemId, List<Integer> orderIds);
}
