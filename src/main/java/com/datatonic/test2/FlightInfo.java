package com.datatonic.test2;

import java.io.Serializable;

/**
 * Created by YaoWang on 2017/12/16.
 *
 * The FlightInfo Class
 */
public class FlightInfo {
    private String date;
    private String airline_code;
    private String route;

    FlightInfo(String date, String airline_code, String route) {
        this.date = date;
        this.airline_code = airline_code;
        this.route = route;
    }

    @Override
    public String toString() {
        return date + "," + airline_code + "," + route;
    }
}
