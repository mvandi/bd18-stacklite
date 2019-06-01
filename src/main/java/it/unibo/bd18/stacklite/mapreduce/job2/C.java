package it.unibo.bd18.stacklite.mapreduce.job2;

public final class C {

    public static final class minmax {
        private static final String base = "minmax";

        public static final class properties {
            private static final String base = minmax.base + ".properties";

            public static final String path = base + ".path";

            public static final String min = base + ".min";

            public static final String max = base + ".max";
        }

        private minmax() {
        }
    }

    private C() {
    }

}
