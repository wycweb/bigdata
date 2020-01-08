package com.wangyichao.bigdata.study.array;


import java.util.Arrays;

/**
 * java.utils.Arrays：操作数组工具类，定义了很多数组操作的方法
 */
public class ArraysUtilTest {

    public static void main(String[] args) {

        //1. boolean equals(int[] a,int[] b) 判断两个数组是否相等
        int[] arr1 = new int[]{1, 2, 3, 4};
        int[] arr2 = new int[]{1, 3, 2, 4};
        boolean isEquals = Arrays.equals(arr1, arr2);
        System.out.println(isEquals);//注意数组是有序的，所以顺序不一致他们也不会是相等的

        //2.String toString[int[] a]:输出数组信息
        System.out.println(Arrays.toString(arr1));

        //3.fill(int[] a,int val):将指定的值填充到数组中
        Arrays.fill(arr1, 10);
        System.out.println(Arrays.toString(arr1));

        //4.void sort[int[] a] 对数组进行排序
        Arrays.sort(arr2);
        System.out.println(Arrays.toString(arr2));

        //5.int binarySearch(int[] a,int key) ：返回数组所在位置，如果找到返回索引，如果未找到返回一个负数
        int[] arr3 = new int[]{-98, 34, 2, 34, 54};
        int index = Arrays.binarySearch(arr3, 2);
        System.out.println(index);
        if (index >= 0) {
            System.out.println("有该元素");
        } else {
            System.out.println("无该元素");
        }


    }
}
