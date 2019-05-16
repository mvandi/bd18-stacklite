package it.unibo.bd18.stacklite;

import java.io.Serializable;

public final class QuestionTag implements Serializable {

    private final int id;
    private final String name;

    public static QuestionTag create(String row) {
        return create(row.split("\\s*,\\s*"));
    }

    public static QuestionTag create(String[] row) {
        return new QuestionTag(
                Utils.readInt(row[0]),
                row[1]);
    }

    public QuestionTag(int id, String name) {
        this.id = id;
        this.name = name;
    }

    public int id() {
        return id;
    }

    public String name() {
        return name;
    }

    public String toCSVString() {
        return String.format("%d,%s", id, name);
    }

}
