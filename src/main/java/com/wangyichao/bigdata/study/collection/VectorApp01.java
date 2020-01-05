package com.wangyichao.bigdata.study.collection;

public class VectorApp01 {

    /**
     * List接口的古老实现类，线程安全的，但是效率很低，底层使用Object[]
     * Vector扩容的方式为初始化值为10
     * 和ArrayList相比
     * 1.Vector当容量不满足时，扩容直接扩容两倍，ArrayList采用扩容1.5倍
     * 2.Vector是线程安全的，但是它的效率比较低
     *
     * 虽然ArrayList是线程不安全的，但是使用Collections.synchronizedList 也能是ArrayList成为线程安全的
     * @param args
     */
    public static void main(String[] args) {

    }
}
