package it.unibo.bd18.stacklite.mapreduce.job1;

import it.unibo.bd18.stacklite.QuestionData;
import it.unibo.bd18.stacklite.QuestionTagData;
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
    protected Class<? extends CombinerBase> getCombinerClass() {
        return Combiner.class;
    }

    @Override
    protected Class<?> getOutputKeyClass() {
        return Text.class;
    }

    @Override
    protected Class<?> getOutputValueClass() {
        return Text.class;
    }

    public static class Combiner extends CombinerBase<Text, Text> {
        private Text keyOut;

        @Override
        protected Text computeOutputKey(QuestionData question, QuestionTagData tag) {
            if (keyOut == null)
                keyOut = new Text(Utils.format(question.creationDate()));
            return keyOut;
        }

        @Override
        protected Text computeOutputValue(QuestionData question, QuestionTagData tag) {
            return new Text(TagScore.format(tag.tag(), question.score()));
        }

        @Override
        protected void preReduce() {
            keyOut = null;
        }
    }

}

