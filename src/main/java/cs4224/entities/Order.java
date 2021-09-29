package cs4224.entities;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import lombok.*;

@Getter
@Setter
@EqualsAndHashCode
@ToString
@Builder
@JsonInclude(Include.NON_NULL)
public class Order {
    private Integer id;
    private Integer warehouseId;
    private Integer districtId;
}
