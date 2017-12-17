package com.datatonic.test2;

/**
 * Created by YaoWang on 2017/12/16.
 *
 * To pad the String with zeros.
 */
public class TestPadString {

    public static void main(String[] args) {

        String string = "1";
        String string1 = string + String.format("%1$0" + (10 - string.length()) + "d", 0);
        System.out.println(string1); // 1000000000
    }
}
