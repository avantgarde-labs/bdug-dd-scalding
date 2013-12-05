package scalding;

import org.apache.commons.cli.Options;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import scala.collection.Iterator;

import java.io.File;
import java.io.IOException;

public class PlainOldWordCount {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private final Text word = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            boolean stopWords = context.getConfiguration().getBoolean("useStopWords", false);
            Iterator<String> stringIterator = LuceneTokenizer.tokenize(value.toString(), stopWords).iterator();

            while (stringIterator.hasNext()) {
                String next = stringIterator.next();
                word.set(next);
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (final IntWritable val: values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Options options = new Options();
        options.addOption("i", "input", true, "input file");
        options.addOption("o", "output", true, "output file");
        options.addOption("s", "stop", false, "use stopwords");
        GenericOptionsParser parser = new GenericOptionsParser(conf, options, args);
        String input = parser.getCommandLine().getOptionValue("input");
        String output = parser.getCommandLine().getOptionValue("output");

        if (output == null || input == null) {
            System.err.println("Usage: PlainOldWordCount: --input <in> --output <out> [--stop]");
            System.exit(2);
        }

        boolean stop = parser.getCommandLine().hasOption("stop");
        conf.setBoolean("useStopWords", stop);

        Job job = new Job(conf, "WordCount");
        job.setJarByClass(PlainOldWordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(true);
    }



    public static void example() throws Exception {
        FileUtils.deleteDirectory(new File("output/grimm-wc-hadoop"));
        main(new String[]{
                "--input", "data/grimm.txt",
                "--output", "output/grimm-wc-hadoop",
                "--stop"
        });
    }
}
