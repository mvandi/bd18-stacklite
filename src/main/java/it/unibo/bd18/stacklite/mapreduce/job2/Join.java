package it.unibo.bd18.stacklite.mapreduce.job2;

import it.unibo.bd18.stacklite.Question;
import it.unibo.bd18.stacklite.QuestionTag;
import it.unibo.bd18.stacklite.mapreduce.AbstractJoin;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

public class Join extends AbstractJoin {

    public Join(Class<?> mainClass, Configuration conf, Path questionsPath, Path questionTagsPath, Path outputPath) {
        super(mainClass, conf, questionsPath, questionTagsPath, outputPath);
    }

    @Override
    protected Class<QuestionMapper> getQuestionMapperClass() {
        return QuestionMapper.class;
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

    public static final class QuestionMapper extends QuestionMapperBase {
        @Override
        protected boolean filter(Question question) {
            return super.filter(question) && question.deletionDate() != null;
        }
    }

    public static class Joiner extends JoinerBase<Text, Text> {
        private Text valueOut;

        @Override
        protected void preReduce() {
            valueOut = null;
        }

        @Override
        protected Text computeOutputKey(Question question, QuestionTag tag) {
            return new Text(tag.name());
        }

        @Override
        protected Text computeOutputValue(Question question, QuestionTag tag) {
            if (valueOut == null)
                valueOut = new Text(question.toCSVString());
            return valueOut;
        }
    }

}
