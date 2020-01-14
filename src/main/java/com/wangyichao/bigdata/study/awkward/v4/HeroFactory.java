package com.wangyichao.bigdata.study.awkward.v4;

public class HeroFactory {

    //通过反射机制，完成输入名字直接实例化对象
    public static SkillImpl getHero(String name) throws Exception {
        /**
         * 反射
         * 元类
         *
         * 对象 类 元类的关系
         * 类是对象的抽象，元类是类的描述
         */
        String classStr = "com.wangyichao.bigdata.study.awkward.v4.hero." + name;
        Class<?> cla = Class.forName(classStr);
        Object obj = cla.newInstance();
        return (SkillImpl) obj;
    }
}
