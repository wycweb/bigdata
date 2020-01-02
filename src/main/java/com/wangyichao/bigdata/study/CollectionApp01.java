package com.wangyichao.bigdata.study;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;

public class CollectionApp01 {

    /**
     * 一：集合的架构：
     * java 集合可分为 Collection和Map两种体系
     * Collection是接口：单列数据，定义了存储一组对象的方法集合，list和set实现了collection接口
     * List接口：元素有序、可重复的集合
     * Set接口：元素无序，不可重复的集合
     * <p>
     * Map接口：双列数据，保存具有映射关系的"key-value对"的结合
     * Hashtable
     * HashMap
     * TreeMap
     * LinkedHashMap
     * Properties
     * <p>
     *
     * 二：集合的抽象方法
     * Collection接口的方法实现：
     * add
     * addAll
     * clear
     * contains
     * containsAll
     * equals
     * hashCode
     * isEmpty
     * iterator
     * remove
     * removeAll
     * retainAll
     * size
     * toArray
     */
    public static void main(String[] args) {
        Collection coll1 = new ArrayList();
        coll1.add("AA");
        coll1.add(1);
        coll1.add(new Date());

        System.out.println(coll1.size());

        Collection coll2 = new ArrayList();
        coll2.add("BB");

        coll1.addAll(coll2);//把coll2全部放在coll1中


        coll1.contains("AA");//判断当前集合是否包含当前元素，返回值布尔类型
        coll1.clear();//清空集合中的元素


    }
}
