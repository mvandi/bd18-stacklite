package it.unibo.bd18.stacklite;

import it.unibo.bd18.util.Pair;
import org.apache.commons.collections.ComparatorUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;

public final class Utils {

    public static final DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

    private static final List<String> headers = Arrays.asList("Id,", ",CreationDate,", ",ClosedDate,", ",DeletionDate,", ",Score,", ",OwnerUserId,", ",AnswerCount", ",Tag");

    public static boolean isHeader(final String row) {
        for (final String header : headers) {
            if (row.contains(header)) {
                return true;
            }
        }
        return false;
    }

    public static <K, V extends Comparable<? super V>> List<Pair<K, V>> sortedByValue(final Map<K, V> m) {
        return sortedByValue(m, true);
    }

    public static <K, V extends Comparable<? super V>> List<Pair<K, V>> sortedByValue(final Map<K, V> m, final boolean ascending) {
        final List<Entry<K, V>> entries = new ArrayList<>(m.entrySet());
        Collections.sort(entries, Utils.<K, V>getComparator(ascending));

        final List<Pair<K, V>> result = new ArrayList<>();
        for (final Entry<K, V> entry : entries)
            result.add(Pair.create(entry.getKey(), entry.getValue()));

        return result;
    }

    public static boolean between(Date d, Date startDate, Date endDate) {
        assert startDate.compareTo(endDate) < 0;
        return d.compareTo(startDate) >= 0 && d.compareTo(endDate) <= 0;
    }

    public static synchronized String format(Date d) {
        final Calendar c = Calendar.getInstance();
        c.setTime(d);
        return String.format("(%d,%d)", c.get(Calendar.YEAR), c.get(Calendar.MONTH) + 1);
    }

    public static void deleteIfExists(FileSystem fs, Path first, Path... more) throws IOException {
        deleteIfExists(fs, false, first, more);
    }

    public static void deleteIfExists(FileSystem fs, boolean recursive, Path first, Path... more) throws IOException {
        deleteIfExists0(fs, recursive, first);
        for (Path path : more) {
            deleteIfExists0(fs, recursive, path);
        }
    }

    private static void deleteIfExists0(FileSystem fs, boolean recursive, Path path) throws IOException {
        if (fs.exists(path)) {
            fs.delete(path, recursive);
        }
    }

    public static synchronized Date readDate(String s) {
        try {
            return s.equals("NA") ? null : df.parse(s);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    public static int readInt(String s) {
        return readInt(s, false);
    }

    public static Integer readInt(String s, boolean boxed) {
        return boxed && s.equals("NA") ? null : Integer.parseInt(s);
    }

    public static synchronized String toString(Date d) {
        return d == null ? "NA" : df.format(d);
    }

    public static String toString(Integer i) {
        return i == null ? "NA" : Integer.toString(i);
    }

    private static <K, V extends Comparable<? super V>> Comparator<Entry<K, V>> getComparator(final boolean ascending) {
        return new Comparator<Entry<K, V>>() {
            private final Comparator<Entry<K, V>> comparator = getComparator();

            private Comparator<Entry<K, V>> getComparator() {
                final Comparator<Entry<K, V>> valueComparator = new Comparator<Entry<K, V>>() {
                    @Override
                    public int compare(Entry<K, V> a, Entry<K, V> b) {
                        return a.getValue().compareTo(b.getValue());
                    }
                };
                return ascending ? valueComparator : ComparatorUtils.reversedComparator(valueComparator);
            }

            @Override
            public int compare(Entry<K, V> a, Entry<K, V> b) {
                return comparator.compare(a, b);
            }
        };
    }

    private Utils() {
    }

}
