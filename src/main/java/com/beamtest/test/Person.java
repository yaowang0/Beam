package com.beamtest.test;

/**
 * Created by YaoWang on 2017/12/14.
 */
public class Person {

    private String name;

    public Person(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }



    @Override
    public boolean equals(Object obj) {
        Person person = (Person) obj;
        String name1 = this.getName();
        String name2 = person.getName();
        return name1.equals(name2);
    }

}
