package it.unibo.bd18.stacklite;

import java.text.ParseException;
import java.util.Date;

import static it.unibo.bd18.stacklite.Utils.df;

public final class C {

    public static class dates {
        public static final Date startDate;
        public static final Date endDate;

        static {
            try {
                startDate = df.parse("2012-01-01T00:00:00Z");
                endDate = df.parse("2012-12-31T23:59:59Z");
            } catch (final ParseException e) {
                throw new RuntimeException(e);
            }
        }

    }

    private C() {
    }

}
