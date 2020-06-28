import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
//import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.A;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Comparator;




public class Hits extends Configured implements Tool {
    public static Double norm_A = 1.0;
    public static Double norm_H = 1.0;
    public static String output;

    class Pair {
        final long a;
        final Double b;

        public Pair(final long a, final Double b) {
            this.a = a;
            this.b = b;
        }
    }

    private ArrayList<Long> top_30(final FileSystem fs, final String filename, final boolean A) throws IOException {
        final ArrayList<Pair> tmp = new ArrayList<Pair>();
        if (A) {
            norm_A =0.;
        } else {
            norm_H = 0.;
        }
        final Path pt = new Path(filename);// Location of file in HDFS
        final BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fs.open(pt)));
        String line;
        while ((line = bufferedReader.readLine()) != null) {
            final String[] split = line.split("\t");
            final Double s = Double.parseDouble(split[1]);
            tmp.add(new Pair(Long.parseLong(split[0]), s));
            if (A) {
                norm_A += s * s;
            } else {
                norm_H += s * s;
            }
        }
        final String fname;
        if (A) {
            norm_A += Math.sqrt(norm_A);
            fname = output + "result_A.txt";
        } else {
            norm_H += Math.sqrt(norm_H);
            fname = output + "result_H.txt";
        }
        final ArrayList<Long> top = new ArrayList<Long>();
        Collections.sort(tmp, new Comparator<Pair>() {
            public int compare(final Pair l, final Pair r) {
                return -Double.compare(l.b, r.b);
            }
        });
        final Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fname), "utf-8"));
        for (int i = 0; i < 30; ++i) {
            top.add(tmp.get(i).a);
            writer.write(String.valueOf(tmp.get(i).a) + "\t" + String.valueOf(tmp.get(i).b)+"\n");
        }
        writer.close();
        return top;
    }

    private static Map<Long, Double> read_lines(final Mapper.Context context, final String filename)
            throws IOException {
        final Map<Long, Double> map = new HashMap<>();

        final Path pt = new Path(filename);
        final FileSystem fs = FileSystem.get(context.getConfiguration());
        final BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fs.open(pt)));
        String line;
        while ((line = bufferedReader.readLine()) != null) {
            final String[] split = line.split("\t");
            map.put(Long.parseLong(split[0]), Double.parseDouble(split[1]));
        }
        return map;
    }

    public static class HitsAMapper extends Mapper<LongWritable, Text, LongWritable, DoubleWritable> {
        Map<Long, Double> urls_with_weights;

        @Override
        protected void setup(final Context context) throws IOException, InterruptedException {
            super.setup(context);
            final Configuration conf = context.getConfiguration();
            final String fname = output + conf.get("epoch") + "/A/part-r-00000";
            urls_with_weights = read_lines(context, fname);
        }

        @Override
        protected void map(final LongWritable key, final Text value, final Context context)
                throws IOException, InterruptedException {
            final Long from = Long.valueOf(value.toString().split("\t")[0]);
            final Long to = Long.valueOf(value.toString().split("\t")[1]);
            final Double s = urls_with_weights.get(to) / norm_A;
            context.write(new LongWritable(from), new DoubleWritable(s));
        }
    }

    public static class HitsHMapper extends Mapper<LongWritable, Text, LongWritable, DoubleWritable> {
        Map<Long, Double> urls_with_weights;

        @Override
        protected void setup(final Context context) throws IOException, InterruptedException {
            super.setup(context);
            final Configuration conf = context.getConfiguration();
            final String fname = output + conf.get("epoch")+ "/H/part-r-00000";
            urls_with_weights = read_lines(context, fname);
        }

        @Override
        protected void map(final LongWritable key, final Text value, final Context context)
                throws IOException, InterruptedException {
            final Long from = Long.valueOf(value.toString().split("\t")[0]);
            final Long to = Long.valueOf(value.toString().split("\t")[1]);
            final Double s = urls_with_weights.get(from) / norm_H;
            context.write(new LongWritable(to), new DoubleWritable(s));
        }
    }

    public static class HitsReducer extends Reducer<LongWritable, DoubleWritable, LongWritable, DoubleWritable> {
        @Override
        protected void reduce(final LongWritable key, final Iterable<DoubleWritable> nums, final Context context)
                throws IOException, InterruptedException {

            Double sum = 0.;
            for (final DoubleWritable val : nums) {
                sum += val.get();
            }
            context.write(key, new DoubleWritable(sum));
        }
    }

    private Job getJobConf_A(final String input, final Integer epoch) throws IOException {

        getConf().set("epoch", String.valueOf(epoch - 1));
        final Job job = Job.getInstance(getConf());

        job.setJarByClass(Hits.class);
        job.setInputFormatClass(TextInputFormat.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setMapOutputKeyClass(LongWritable.class);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(
                output + String.valueOf(epoch) + "/H"));

        job.setMapperClass(HitsAMapper.class);
        job.setReducerClass(HitsReducer.class);

        return job;
    }

    private Job getJobConf_B(final String input, final Integer epoch) throws IOException {

        getConf().set("epoch", String.valueOf(epoch - 1));
        final Job job = Job.getInstance(getConf());

        job.setJarByClass(Hits.class);
        job.setInputFormatClass(TextInputFormat.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setMapOutputKeyClass(LongWritable.class);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(
                output + String.valueOf(epoch) + "/A"));

        job.setMapperClass(HitsHMapper.class);
        job.setReducerClass(HitsReducer.class);

        return job;
    }

    @Override
    public int run(final String[] args) throws Exception {
        final FileSystem fs = FileSystem.get(getConf());
        int res = 0;
        output=args[1]+"/";
        ArrayList<Long> old_A = new ArrayList<>(30);
        ArrayList<Long> old_H = new ArrayList<>(30);
        for (int epoch = 0; epoch < 30; epoch++) {
            {
                if (fs.exists(new Path(output + String.valueOf(epoch)))){
                    final String fname_A = output + String.valueOf(epoch) + "/A/part-r-00000";
                    final String fname_H = output + String.valueOf(epoch) + "/H/part-r-00000";
                    old_A = top_30(fs,fname_A, true);
                    old_H = top_30(fs,fname_H, false);
                    continue;
                }
                final Job job = getJobConf_A(args[0], epoch);
                res += job.waitForCompletion(true) ? 0 : 1;
            }
            {
                final Job job = getJobConf_B(args[0], epoch);
                res += job.waitForCompletion(true) ? 0 : 1;
            }
            final String fname_A = output + String.valueOf(epoch) + "/A/part-r-00000";
            final String fname_H = output + String.valueOf(epoch) + "/H/part-r-00000";
            final ArrayList<Long>  new_A = top_30(fs,fname_A, true);
            final ArrayList<Long>   new_H = top_30(fs,fname_H, false);
            if(old_A.containsAll(new_A) & old_H.containsAll(new_H)) 
                    break;
            old_A=new_A;
            old_H=new_H;

        }
        return res;
    }

    static public void main(final String[] args) throws Exception {
        final int ret = ToolRunner.run(new Hits(), args);
        System.exit(ret);
    }
}
