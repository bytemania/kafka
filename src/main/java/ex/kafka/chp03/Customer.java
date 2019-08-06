package ex.kafka.chp03;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@RequiredArgsConstructor
@Getter
@ToString
public class Customer {

    private final int id;
    private final String name;
    private final String email;
}
