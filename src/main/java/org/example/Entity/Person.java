package org.example.Entity;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor

public class Person {
    private String id;
    private String createTime;
    private String name;
    private boolean isBlocked;
    private String gender;
    private String country;
    private String city;
    private String birthday;
}
