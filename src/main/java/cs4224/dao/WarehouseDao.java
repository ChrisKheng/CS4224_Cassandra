package cs4224.dao;

import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.Query;
import com.datastax.oss.driver.api.mapper.annotations.Update;
import cs4224.entities.Warehouse;

import java.math.BigDecimal;

import static com.datastax.oss.driver.api.mapper.entity.saving.NullSavingStrategy.DO_NOT_SET;

@Dao
public interface WarehouseDao {

    @Query("SELECT * FROM ${qualifiedTableId} WHERE W_ID = :id")
    Row getById(int id);

    @Update(customWhereClause = "W_ID = :id IF w_ytd = :w_ytd", nullSavingStrategy = DO_NOT_SET)
    void updateWhereIdEquals(Warehouse warehouse, int id, BigDecimal w_ytd);
}
