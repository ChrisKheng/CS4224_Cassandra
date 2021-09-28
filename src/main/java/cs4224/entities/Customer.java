package cs4224.entities;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.*;

import java.util.Date;

@Getter
@Setter
@EqualsAndHashCode
@ToString
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Customer {
    private int id;
    private int warehouseId;
    private int districtId;
    private String firstName;
    private String middleName;
    private String lastName;
    private String street1;
    private String street2;
    private String city;
    private String state;
    private String zip;
    private String phone;
    private Date entryCreateDateTime;
    private String creditStatus;
    private double creditLimit;
    private double discountRate;
    private double balance;
    private float paymentYTD;
    private int numPayments;
    private int numDeliveries;
    private String miscData;
}
