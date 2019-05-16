package it.unibo.bd18.stacklite;

import java.io.Serializable;
import java.util.Date;

public final class Question implements Serializable {

    private final int id;
    private final Date creationDate;
    private final Date closedDate;
    private final Date deletionDate;
    private final int score;
    private final Integer ownerUserId;
    private final Integer answerCount;

    public static Question create(String row) {
        return create(row.split("\\s*,\\s*"));
    }

    public static Question create(String[] row) {
        return new Question(
                Utils.readInt(row[0]),
                Utils.readDate(row[1]),
                Utils.readDate(row[2]),
                Utils.readDate(row[3]),
                Utils.readInt(row[4]),
                Utils.readInt(row[5], true),
                Utils.readInt(row[6], true));
    }

    public Question(int id, Date creationDate, Date closedDate, Date deletionDate, int score, Integer ownerUserId, Integer answerCount) {
        this.id = id;
        this.creationDate = creationDate;
        this.closedDate = closedDate;
        this.deletionDate = deletionDate;
        this.score = score;
        this.ownerUserId = ownerUserId;
        this.answerCount = answerCount;
    }

    public int id() {
        return id;
    }

    public Date creationDate() {
        return creationDate;
    }

    public Date closedDate() {
        return closedDate;
    }

    public Date deletionDate() {
        return deletionDate;
    }

    public int score() {
        return score;
    }

    public Integer ownerUserId() {
        return ownerUserId;
    }

    public Integer answerCount() {
        return answerCount;
    }

    public String toCSVString() {
        return String.format("%d,%s,%s,%s,%d,%s,%s",
                id,
                Utils.toString(creationDate),
                Utils.toString(closedDate),
                Utils.toString(deletionDate),
                score,
                Utils.toString(ownerUserId),
                Utils.toString(answerCount));
    }

}
