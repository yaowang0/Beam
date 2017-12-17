package com.datatonic.test;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by YaoWang on 2017/12/14.
 */
public class Test {
    public static void main(String[] args) {

//        String str = "1";
//        DecimalFormat df = new DecimalFormat("0000000000");
//        String string = df.format(Integer.parseInt(str));
//        String string1 = str + String.format("%1$0" + (10 - str.length()) + "d", 0);
//        System.out.println(string1);

        Set<Person> set = new HashSet<>();
        Person person1 = new Person("xiaohua");
        Person person2 = new Person("xiaoming");
        Person person3 = new Person("xiaohua");
        set.add(person1);
        set.add(person2);
        set.add(person3);
        Set<String> set1 = new HashSet<>();
        set1.add("xiaohua");
        set1.add("xiaoming");
        set1.add("xiaohua");

        List<Integer> list = new ArrayList<>();
        list.add(1);
        list.add(4);
        list.add(3);
        list.add(2);
        System.out.println(list.get(list.size() / 2 - 1));
    }
}
