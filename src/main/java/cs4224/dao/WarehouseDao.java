package cs4224.dao;

import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.Query;
import com.datastax.oss.driver.api.mapper.annotations.Update;
import cs4224.entities.Warehouse;

import static com.datastax.oss.driver.api.mapper.entity.saving.NullSavingStrategy.DO_NOT_SET;

@Dao
public interface WarehouseDao {

    @Query("SELECT * FROM ${qualifiedTableId} WHERE W_ID = :w_id")
    Warehouse getById(@CqlName("w_id") int id);

    @Update(customWhereClause = "W_ID = :id", nullSavingStrategy = DO_NOT_SET)
    void updateWhereIdEquals(Warehouse warehouse, int id);
}
