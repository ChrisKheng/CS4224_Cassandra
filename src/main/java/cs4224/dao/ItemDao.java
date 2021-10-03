package cs4224.dao;

import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.Query;
import cs4224.entities.item.ItemName;

@Dao
public interface ItemDao {

    @Query("SELECT I_NAME FROM ${qualifiedTableId} WHERE I_ID = :i_id")
    ItemName getNameById(@CqlName("i_id") int id);

}
