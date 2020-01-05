package com.wangyichao.bigdata.study.collection;

public class LinkedListApp01 {

    /**
     * 对于频繁的插入、删除操作，使用此类List会比ArrayList高，底层使用双向链表存储
     * 内部声明了Node类型的first和last属性，默认值为null
     * list.add(123) 将123封装到node中，创建了node对象
     * node定义的结构
     *     private static class Node<E> {
     *         E item;
     *         Node<E> next;
     *         Node<E> prev;
     *
     *         Node(Node<E> prev, E element, Node<E> next) {
     *             this.item = element;
     *             this.next = next;
     *             this.prev = prev;
     *         }
     *     }
     *
     * Node为双向链表的结构，体现了LinkedList的数据特点
     * 它不涉及到扩容的问题，使用上较为灵活
     */
    public static void main(String[] args) {

    }
}
