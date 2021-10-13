package cs4224.dao;

import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.Query;

@Dao
public interface ItemDao {

    @Query("SELECT I_NAME FROM ${qualifiedTableId} WHERE I_ID = :id")
    Row getNameById(int id);

}
