package it.unibo.bd18.util;

public final class Pair<L, R> {

    private final L left;
    private final R right;

    public static <L, R> Pair<L, R> create(L left, R right) {
        return new Pair<>(left, right);
    }

    private Pair(L left, R right) {
        this.left = left;
        this.right = right;
    }

    public L left() {
        return left;
    }

    public R right() {
        return right;
    }

    @Override
    public String toString() {
        return String.format("(%s,%s)", left, right);
    }

}
