package com.oracle.test;

/**
 * Created by YaoWang on 2017/12/19.
 */
public class Product {

    private String key;

    public Product() {

    }

    public Product(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }

    @Override
    public String toString() {

        return key;
    }
}
