package models;


import lombok.Builder;
import lombok.Value;
import lombok.extern.log4j.Log4j2;

@Log4j2
@Value
@Builder(toBuilder = true)
public class Employee {

    String name;
    Integer salary;
    String email;
}
