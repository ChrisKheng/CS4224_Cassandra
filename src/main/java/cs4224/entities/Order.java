package cs4224.entities;

import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.fasterxml.jackson.annotation.JsonInclude;
import cs4224.mapper.CQLMapper;
import lombok.*;

import java.math.BigDecimal;
import java.time.Instant;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
@Entity
@CqlName("Orders")
public class Order {

    @CqlName("o_id")
    private Integer id;

    @CqlName("o_w_id")
    private Integer warehouseId;

    @CqlName("o_d_id")
    private Integer districtId;

    @CqlName("o_c_id")
    private Integer customerId;

    @CqlName("o_carrier_id")
    private Integer carrierId;

    @CqlName("o_ol_cnt")
    private BigDecimal numItems;

    @CqlName("o_all_local")
    private BigDecimal allLocal;

    @CqlName("o_entry_d")
    private Instant entryDateTime;

    public static Order map(Row row) {
        final CQLMapper cqlMapper = new CQLMapper();
        final Order order = new Order();
        order.setId(cqlMapper.mapInt(row, "o_id"));
        order.setWarehouseId(cqlMapper.mapInt(row,"o_w_id"));
        order.setDistrictId(cqlMapper.mapInt(row,"o_d_id"));
        order.setCustomerId(cqlMapper.mapInt(row, "o_c_id"));
        order.setCarrierId(cqlMapper.mapInt(row, "o_carrier_id"));
        order.setNumItems(cqlMapper.mapBigDecimal(row, "o_ol_cnt"));
        order.setAllLocal(cqlMapper.mapBigDecimal(row, "o_all_local"));
        order.setEntryDateTime(cqlMapper.mapInstant(row, "o_entry_d"));
        return order;
    }
}
