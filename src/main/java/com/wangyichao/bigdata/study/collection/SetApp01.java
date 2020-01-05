package com.wangyichao.bigdata.study.collection;

public class SetApp01 {

    /**
     * Set接口是Collection子接口，set接口没有提供额外的方法
     * Set集合不允许包含相同的元素，如果试把两个相同的元素加入同一个Set集合中，则添加操作失败
     * Set判断两个对象是否相同不是使用 == 运算符，而是使用equals方法
     * <p>
     * Set主要有三种实现
     * HashSet：作为Set接口的主要实现类，是线程不安全的，可以存储null值
     * LinkedHashSet：是HashSet的子类，在HashSet基础上，处理成链表结构，会使遍历内部数据时，可以按照添加的顺序去遍历
     * TreeSet：底层使用红黑树进行存储，使得TreeSet中的数据，必须是同一个类的数据，这样使得内部的数据可以按照一定顺序进行排序
     */
    public static void main(String[] args) {

    }
}
