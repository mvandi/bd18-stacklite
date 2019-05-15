package it.unibo.bd18.util;

import org.apache.commons.lang.ClassUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import static it.unibo.bd18.util.ClassHelper.*;
import static org.junit.Assert.assertEquals;

public class ClassHelperTest {

    interface I {
    }

    static class A {
    }

    static class B extends A implements I {
    }

    static class C extends A {
    }

    static class D extends B {
    }

    static class E extends C implements I {
    }

    static class F extends C {
    }

    static class G implements I {
    }

    @Test
    public void test() {
        assertEquals(A.class, commonSuperclass(A.class, B.class));
        assertEquals(A.class, commonSuperclass(B.class, A.class));
        assertEquals(A.class, commonSuperclass(A.class, D.class));
        assertEquals(A.class, commonSuperclass(D.class, A.class));
        assertEquals(A.class, commonSuperclass(C.class, D.class));
        assertEquals(A.class, commonSuperclass(D.class, C.class));
        assertEquals(C.class, commonSuperclass(E.class, F.class));
        assertEquals(C.class, commonSuperclass(F.class, E.class));
        assertEquals(Object.class, commonSuperclass(Number.class, String.class));
    }

    @Test
    public void testInterfaces() {
        System.out.println(ClassUtils.isAssignable(D.class, I.class));
        System.out.println(commonInterfaces(Text.class, IntWritable.class, LongWritable.class));
    }

}