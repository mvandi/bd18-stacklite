package it.unibo.bd18.util;

import org.apache.commons.lang.ClassUtils;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;

public final class ClassHelper {

    public static Class<?> commonSuperclass(Iterable<? extends Class<?>> classes) {
        final Iterator<? extends Class<?>> it = classes.iterator();
        if (!it.hasNext())
            return null;
        Class<?> result = it.next();
        while (result != null && it.hasNext()) {
            result = commonSuperclass0(result, it.next());
        }
        return result;
    }

    public static Class<?> commonSuperclass(Class<?> a, Class<?> b, Class<?>... more) {
        Class<?> result = commonSuperclass0(a, b);
        for (int i = 0; result != null && i < more.length; i++) {
            result = commonSuperclass0(result, more[i]);
        }
        return result;
    }

    private static Class<?> commonSuperclass0(Class<?> a, Class<?> b) {
        if (a == null)
            return null;
        if (b == null)
            return null;
        if (ClassUtils.isAssignable(a, b))
            return b;

        do {
            if (ClassUtils.isAssignable(b, a))
                return a;
            a = a.getSuperclass();
        } while (a != null);

        return null;
    }

    public static Set<Class<?>> commonInterfaces(Iterable<? extends Class<?>> classes) {
        final Iterator<? extends Class<?>> it = classes.iterator();
        if (!it.hasNext())
            return Collections.emptySet();

        final Set<Class<?>> result = getAllInterfaces(it.next());
        while (!result.isEmpty() && it.hasNext())
            result.retainAll(getAllInterfaces(it.next()));

        return result;
    }

    public static Set<Class<?>> commonInterfaces(Class<?> first, Class<?>... more) {
        final Set<Class<?>> result = getAllInterfaces(first);
        for (int i = 0; !result.isEmpty() && i < more.length; i++)
            result.retainAll(getAllInterfaces(more[i]));

        return result;
    }

    private static Set<Class<?>> getAllInterfaces(Class<?> cls) {
        return getAllInterfaces(cls, false);
    }

    private static Set<Class<?>> getAllInterfaces(Class<?> cls, boolean recursive) {
        final Set<Class<?>> classes = new LinkedHashSet<>();

        if (cls.isInterface())
            classes.add(cls);

        do {
            final Class<?>[] interfaces = cls.getInterfaces();
            Collections.addAll(classes, interfaces);
            if (recursive) {
                for (final Class<?> iface : interfaces)
                    classes.addAll(getAllInterfaces(iface, true));
            }
            cls = cls.getSuperclass();
        } while (cls != null);
        return classes;
    }

}
