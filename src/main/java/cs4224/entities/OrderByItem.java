package cs4224.entities;

import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.fasterxml.jackson.annotation.JsonInclude;
import cs4224.mapper.CQLMapper;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
@Entity
@CqlName("Order_By_Item")
public class OrderByItem {

    @CqlName("o_id")
    private Integer orderId;

    @CqlName("o_w_id")
    private Integer warehouseId;

    @CqlName("o_d_id")
    private Integer districtId;

    @CqlName("i_id")
    private Integer itemId;

    public static OrderByItem map(Row row) {
        final CQLMapper cqlMapper = new CQLMapper();
        final OrderByItem orderByItem = new OrderByItem();
        orderByItem.setOrderId(cqlMapper.mapInt(row, "o_id"));
        orderByItem.setWarehouseId(cqlMapper.mapInt(row,"o_w_id"));
        orderByItem.setDistrictId(cqlMapper.mapInt(row,"o_d_id"));
        orderByItem.setItemId(cqlMapper.mapInt(row, "i_id"));
        return orderByItem;
    }
}
