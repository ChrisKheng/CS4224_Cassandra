package cs4224.entities;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.*;

@Getter
@Setter
@EqualsAndHashCode
@ToString
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class District {
    private int id;
    private int warehouseId;
    private int name;
    private String street1;
    private String street2;
    private String city;
    private String state;
    private String zip;
    private double tax;
    private double amountPaidYTD;
    private int nextOrderId;
}
