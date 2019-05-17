package it.unibo.bd18.stacklite.mapreduce.job1;

import it.unibo.bd18.stacklite.Question;
import it.unibo.bd18.stacklite.QuestionTag;
import it.unibo.bd18.stacklite.Utils;
import it.unibo.bd18.stacklite.mapreduce.AbstractJoin;
import it.unibo.bd18.stacklite.mapreduce.TagScore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

public final class Join extends AbstractJoin {

    public Join(Class<?> mainClass, Configuration conf, Path questionsPath, Path questionTagsPath, Path outputPath) {
        super(mainClass, conf, questionsPath, questionTagsPath, outputPath);
    }

    @Override
    protected Class<Joiner> getJoinerClass() {
        return Joiner.class;
    }

    @Override
    protected Class<Text> getOutputKeyClass() {
        return Text.class;
    }

    @Override
    protected Class<Text> getOutputValueClass() {
        return Text.class;
    }

    public static class Joiner extends JoinerBase<Text, Text> {
        private Text keyOut;

        @Override
        protected void preReduce() {
            keyOut = null;
        }

        @Override
        protected Text computeOutputKey(Question question, QuestionTag tag) {
            if (keyOut == null)
                keyOut = new Text(Utils.format(question.creationDate()));
            return keyOut;
        }

        @Override
        protected Text computeOutputValue(Question question, QuestionTag tag) {
            return new Text(TagScore.format(tag.name(), question.score()));
        }
    }

}
