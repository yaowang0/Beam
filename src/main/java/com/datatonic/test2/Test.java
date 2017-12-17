package com.datatonic.test2;

/**
 * Created by YaoWang on 2017/12/16.
 */
public class Test {

    public static void main(String[] args) {

        FlightInfo flightInfo1 = new FlightInfo("2016/01/01","111", "shanghaitolondon");
        FlightInfo flightInfo2 = new FlightInfo("2016/01/01","111", "shanghaitolondon");
        System.out.println(flightInfo1.equals(flightInfo2));
    }
}
