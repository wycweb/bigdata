package com.wangyichao.bigdata.study.collection;

import java.util.*;

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
     * <p>
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
        coll1.add(123);
        coll1.add(456);
        coll1.add(new Date());

        System.out.println(coll1.size());

        Collection coll2 = new ArrayList();
        coll2.add("BB");

        coll1.addAll(coll2);//把coll2全部放在coll1中


        // 1.contains(Object obj):判断当前集合中是否包含obj
        coll1.contains("AA");//判断当前集合是否包含当前元素，返回值布尔类型

        // 2.containsAll(Collection coll1)
        Collection coll3 = Arrays.asList(123, 456);

        //coll3是否全包含在coll1内
        System.out.println(coll1.containsAll(coll3));

        System.out.println(coll1.remove(123));//移除元素
        coll1.clear();//清空集合中的元素

        // 4. removeAll(Collection coll1) 差集操作：从当前集合中移除coll1中所有的元素
        // 5. retainAll(Collection coll1) 交集操作
        // 6. equals(Object obj)          两个集合必须一模一样，ArrayList必须顺序都一样，HashSet可以顺序不同
        // 7. hashCode 返回当前对象的哈希值
        // 8. toArry 将集合转换成数组，数组变集合asList
        Object[] arr = coll3.toArray();


        iteratorTest();

    }

    /*
     * 9. iterator():返回Iterator接口的实例，用于遍历集合元素。
     * iterator对象成绩为迭代器(设计模式的一种)，主要用于遍历Collection集合中的元素
     *
     */
    private static void iteratorTest() {
        Collection coll1 = new ArrayList();
        coll1.add("AA");
        coll1.add(123);
        coll1.add(456);
        coll1.add(new Date());
        coll1.add(false);

        //方式一
        Iterator iterator = coll1.iterator();
        System.out.println(iterator.next());

        //方式二
        for (int i = 0; i < coll1.size(); i++) {
            System.out.println(iterator.next());
        }

        //方式三，开发过程中这种方式会多一点
        while (iterator.hasNext()) {
            System.out.println(iterator.next());
        }

        //删除集合中的 "AA",remove操作时，注意指针的位置，否则可能会出现IllegalStateException的错误
        while (iterator.hasNext()) {

            Object object = iterator.next();
            if ("AA".equals(object)){
                iterator.remove();
            }
        }

    }
}
