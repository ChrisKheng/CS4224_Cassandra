package cs4224.entities.order;

import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.fasterxml.jackson.annotation.JsonInclude;
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
}
