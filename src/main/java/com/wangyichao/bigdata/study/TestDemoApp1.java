package com.wangyichao.bigdata.study;

/**
 * 随手测试代码使用
 */
public class TestDemoApp1 {
    public static void main(String[] args) {
        int oldCapacity = 101;
        int newCapacity = oldCapacity + (oldCapacity >> 1);

        System.out.println(newCapacity);
        System.out.println(101 * 1.5);
    }
}
