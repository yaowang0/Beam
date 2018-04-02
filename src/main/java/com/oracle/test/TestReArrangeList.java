package com.oracle.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by YaoWang on 2017/12/19.
 */
public class TestReArrangeList {
    public static void main(String[] args) {

        Product p1 = new Product("Apple");
        Product p2 = new Product("Pear");
        Product p3 = new Product("Orange");
        Product p4 = new Product("Banana");
        Product p5 = new Product("WaterMelon");
        Product p6 = new Product("Peach");

        List<String> keys = Arrays.asList("Apple", "Banana", "Peach", "Pear", "Orange", "WaterMelon");
        List<Product> products = Arrays.asList(p1, p2, p3, p4, p5, p6);



    }

    public List<Product> sortByKeyList(List<Product> products, List<String> keys) {
        List<Product> result = new ArrayList<>();
        if (products.size() > 0) {
            for(int i= 0; i < products.size() ;i++){
                result.add(new Product());
            }
            for (Product product : products) {
                String key = product.getKey();
                result.set(keys.indexOf(key), product);
            }
        }
        return result;
    }

    public boolean makeRow(int small, int big, int goal) {

        return small == goal || small + big * 5 == goal || big * 5 == goal;
    }

    public int pontoon(int a, int b) {
        if (a > 21 && b > 21) {
            return 0;
        } else if ((a - 21) * (b - 21) < 0) {
            return (a - b) < 0 ? a : b;
        } else if ((a - 21) * (b - 21) == 0) {
            return (a - 21) == 0 ? a : b;
        } else {
            return (a > b) ? a : b;
        }
    }

    public boolean splitInts(int[] nums) {
        int[] leftArray = new int[nums.length];
        for (int i = 0; i < nums.length - 1; i++) {
            leftArray[i] = nums[i];
            nums[i] = 0;
            if (sum(leftArray) == sum(nums)) {
                return true;
            }
        }
        return false;
    }

    public int sum(int[] nums) {
        int result = 0;
        for (int i : nums) {
            result += i;
        }
        return result;
    }

}
