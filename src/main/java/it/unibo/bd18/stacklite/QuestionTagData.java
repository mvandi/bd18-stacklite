package it.unibo.bd18.stacklite;

import java.io.Serializable;

public final class QuestionTagData implements Serializable {

    private final int id;
    private final String tag;

    public static QuestionTagData create(String row) {
        return create(row.split("\\s*,\\s*"));
    }

    public static QuestionTagData create(String[] row) {
        return new QuestionTagData(
                Utils.readInt(row[0]),
                row[1]);
    }

    public QuestionTagData(int id, String tag) {
        this.id = id;
        this.tag = tag;
    }

    public int id() {
        return id;
    }

    public String tag() {
        return tag;
    }

    public String toCSVString() {
        return String.format("%d,%s", id, tag);
    }

}
