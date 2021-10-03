package cs4224.mapper;

import com.datastax.oss.driver.api.mapper.annotations.DaoFactory;
import com.datastax.oss.driver.api.mapper.annotations.DaoTable;
import com.datastax.oss.driver.api.mapper.annotations.Mapper;
import cs4224.dao.ItemDao;
import cs4224.dao.OrderDao;

@Mapper
public interface ItemMapper {

    @DaoFactory
    ItemDao dao(@DaoTable String table);
}
