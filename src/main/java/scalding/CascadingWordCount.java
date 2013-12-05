package scalding;

import cascading.flow.*;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.flow.local.LocalFlowConnector;
import cascading.operation.*;
import cascading.operation.aggregator.Count;
import cascading.pipe.*;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.util.Pair;
import org.apache.commons.cli.*;
import scala.collection.Iterator;

import java.util.Properties;


public class CascadingWordCount {

    public static class Tokenizer extends BaseOperation<Pair<Boolean, Tuple>> implements Function<Pair<Boolean, Tuple>> {

        private final boolean withStopWords;

        public Tokenizer(Fields fields, boolean withStopWords) {
            super(1, fields);
            this.withStopWords = withStopWords;
        }

        @Override
        public void prepare(FlowProcess flowProcess, OperationCall<Pair<Boolean, Tuple>> operationCall) {
            operationCall.setContext(new Pair<>(this.withStopWords, Tuple.size(1)));
        }

        @Override
        public void operate(FlowProcess flowProcess, FunctionCall<Pair<Boolean, Tuple>> functionCall) {
            final String value = functionCall.getArguments().getString(0);
            if (value != null) {
                Boolean stopWords = functionCall.getContext().getLhs();
                Iterator<String> stringIterator = LuceneTokenizer.tokenize(value, stopWords).iterator();

                while (stringIterator.hasNext()) {
                    String next = stringIterator.next();
                    functionCall.getContext().getRhs().set(0, next);
                    functionCall.getOutputCollector().add(functionCall.getContext().getRhs());
                }
            }
        }
    }

    public static void main(String[] args) throws ParseException {

        Options options = new Options();
        options.addOption("i", "input", true, "input file");
        options.addOption("o", "output", true, "output file");
        options.addOption("s", "stop", false, "use stopwords");
        options.addOption("l", "local", false, "use local mode instead of hdfs-local");

        CommandLine parse = new GnuParser().parse(options, args);
        String input = parse.getOptionValue("input");
        String output = parse.getOptionValue("output");

        if (output == null || input == null) {
            System.err.println("Usage: CascadingWordCount: --input <in> --output <out> [--stop]");
            System.exit(2);
        }

        boolean isLocal = parse.hasOption("local");

        Properties properties = new Properties();
        AppProps.setApplicationJarClass(properties, CascadingWordCount.class);

        FlowConnector flowConnector = isLocal ?
                new LocalFlowConnector() : new HadoopFlowConnector();

        Tap docTap = isLocal ?
                new FileTap(new cascading.scheme.local.TextLine(), input) :
                new Hfs(new TextLine(), input);

        Tap wcTap = isLocal ?
                new FileTap(new cascading.scheme.local.TextLine(), output) :
                new Hfs(new TextLine(), output);


        Fields token = new Fields("word");
        Fields text = new Fields("line");
        Tokenizer tokenizer = new Tokenizer(token, parse.hasOption("stop"));
        Pipe docPipe = new Each("word", text, tokenizer, Fields.RESULTS);

        Pipe wcPipe = new Pipe("wc", docPipe);
        wcPipe = new GroupBy(wcPipe, token);
        wcPipe = new Every(wcPipe, Fields.ALL, new Count(), Fields.ALL);

        FlowDef flowDef = FlowDef.flowDef()
                .setName("wc")
                .addSource(docPipe, docTap)
                .addTailSink(wcPipe, wcTap);

        flowConnector.connect(flowDef).complete();
    }

    public static void example() throws ParseException {
        main(new String[]{"--local", "--stop",
                "--input", "data/grimm.txt",
                "--output", "output/grimm-wc-cascading.txt"});
    }
}
