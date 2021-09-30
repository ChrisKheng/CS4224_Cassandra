package cs4224.mapper;

import com.datastax.oss.driver.api.mapper.annotations.DaoFactory;
import com.datastax.oss.driver.api.mapper.annotations.DaoTable;
import com.datastax.oss.driver.api.mapper.annotations.Mapper;
import cs4224.dao.CustomerDao;
import cs4224.dao.OrderLineDao;

@Mapper
public interface OrderLineMapper {

    @DaoFactory
    OrderLineDao dao(@DaoTable String table);
}
