package cs4224.entities;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.*;

@Getter
@Setter
@EqualsAndHashCode
@ToString
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Warehouse {
    private int id;
    private String name;
    private String street1;
    private String street2;
    private String city;
    private String state;
    private String zip;
    private double tax;
    private double amountPaidYTD;
}
