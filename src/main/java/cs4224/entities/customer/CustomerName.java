package cs4224.entities.customer;

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
@CqlName("customer")
public class CustomerName {

    @CqlName("c_first")
    private String firstName;

    @CqlName("c_middle")
    private String middleName;

    @CqlName("c_last")
    private String lastName;
}
