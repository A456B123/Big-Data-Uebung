package utils_steger;

import java.util.LinkedList;
import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;



public class ADSBSplitter implements MapFunction<String, ADSBMessage> {
    private static final long serialVersionUID = 0;

    @Override
    public ADSBMessage map(String line) throws Exception {
        List<String> words = new LinkedList<String>();
        for (String word : line.split(",")) {
            words.add(word);
        }
        return new ADSBMessage(words);
    }

}

/*
public class ADSBSplitter implements FlatMapFunction<String, ADSBMessage> {
    private static final long serialVersionUID = 5925981468150038617L;

    @Override
    public void flatMap(
            String sentence,
            Collector<ADSBMessage> out) throws Exception {
        List<String> words = new LinkedList<String>();
        for (String word : sentence.split(",")) {
            words.add(word);
        }
        out.collect(new ADSBMessage(words));
    }
}
*/
