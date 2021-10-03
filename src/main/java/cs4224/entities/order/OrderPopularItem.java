package cs4224.entities.order;

import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.*;

import java.time.Instant;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
@Entity
@CqlName("Orders")
public class OrderPopularItem {

    @CqlName("o_id")
    private Integer id;

    @CqlName("o_c_id")
    private Integer customerId;

    @CqlName("o_entry_d")
    private Instant entryDateTime;
}
