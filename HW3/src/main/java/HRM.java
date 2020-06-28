import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;




public class HRM extends Configured implements Tool{
    public static final String TERMs = "terms";
    public static final String DOCs = "docs";
    public static final String CLICKs = "click";


    static private Map<String, Integer> get_url(final Mapper.Context context, final Path pt) {
        final Map<String, Integer> map = new HashMap<>();
        try {
            final FileSystem fs = FileSystem.get(context.getConfiguration());
            final BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fs.open(pt)));
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                final String[] split = line.split("\t");
                final String s = split[1].charAt(split[1].length() - 1) == '/'
                        ? split[1].substring(0, split[1].length() - 1)
                        : split[1];
                map.put(s, Integer.parseInt(split[0]));
            }
        } catch (final Exception e) {
            e.printStackTrace();
        }

        return map;
    }

    static private Map<Integer, String> get_queries(final Mapper.Context context, final Path pt,
            final Map<String, String> inv_q) {
        final Map<Integer, String> map = new HashMap<>();
        try {
            final FileSystem fs = FileSystem.get(context.getConfiguration());
            final BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fs.open(pt)));
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                final String[] split = line.split("\t");
                map.put(Integer.parseInt(split[0]), split[1].trim());
                if (inv_q != null)
                    inv_q.put(split[1].trim(), split[0]);
            }
        } catch (final IOException e) {
            e.printStackTrace();
        }

        return map;
    }

    public static class HRMMapper extends Mapper<LongWritable, Text, Text, Text> {
        static final IntWritable one = new IntWritable(1);
        static final IntWritable null_0 = new IntWritable(0);
        static Map<String, Integer> ids;
        static Map<Integer, String> queries;
        static Map<Integer, Integer> query_url = new HashMap<>();
        static boolean QD = true;
        final boolean host = false;
        Map<String, String> inv_q = new HashMap<>();

        @Override
        protected void setup(final Context context) throws IOException, InterruptedException {
            super.setup(context);
            final Path urls = new Path("check/url.data/url.data");
            final Path q = new Path("check/queries.tsv");
            ids = get_url(context, urls);
            queries = get_queries(context, q, inv_q);
        }

        @Override
        protected void map(final LongWritable key, final Text value, final Context context)
                throws IOException, InterruptedException {
            try {
                final QRecord record = new QRecord(host);
                record.parseString(value.toString());
                for (int i = 0; i < record.shownLinks.size(); i++) {
                    if (ids.containsKey(record.shownLinks.get(i))){
                        final String[] terms = record.query.split("\\ ");
                        for (final String term : terms) {
                            context.write(new Text(term + "|1"), new Text(String.valueOf(ids.get(record.shownLinks.get(i)))));
                            context.write(new Text(String.valueOf(ids.get(record.shownLinks.get(i))) + "|0"),new Text(term));
                            if(record.clickedLinks.contains(record.shownLinks.get(i))){
                                context.write(new Text(String.valueOf(ids.get(record.shownLinks.get(i))) +"\t"+term+ "|-1"),new Text("1"));
                            }
                        }
                    }
                }
            } catch (final Exception e) {
                e.printStackTrace();
                return;
            }
        }

    }

    public static class HRMReducer extends Reducer<Text, Text, Text, IntWritable> {

        private MultipleOutputs<Text, IntWritable> multipleOutputs;

        public void setup(final Reducer.Context context) {
            multipleOutputs = new MultipleOutputs(context);

        }

        @Override
        protected void reduce(final Text key, final Iterable<Text> nums, final Context context)
                throws IOException, InterruptedException {
                int sum=0;
                final String[] split=key.toString().split("\\|");
                if(Integer.valueOf(split[1])!=-1){
                    final Set<String> set = new HashSet<>();
                    for (final Text i : nums) {
                        if(!set.contains(i.toString())){
                            sum+=1;
                            set.add(i.toString());
                        }
                    }
                }else{
                    for (final Text _ : nums) {
                            sum+=1;
                    }
                }
                if(Integer.valueOf(split[1])==1){
                    multipleOutputs.write(new Text(split[0]), new IntWritable(sum), TERMs+"/part");
                }else{
                    if(Integer.valueOf(split[1])==0)
                        multipleOutputs.write(new Text(split[0]), new IntWritable(sum), DOCs+"/part");
                    else
                    multipleOutputs.write(new Text(split[0]), new IntWritable(sum), CLICKs+"/part");
                }
        }

        public void cleanup(final Context context) throws IOException, InterruptedException {
            super.cleanup(context);
            multipleOutputs.close();
        }
    }

    private Job getJobConf(final String input, final String output) throws IOException {

        final Job job = Job.getInstance(getConf());
        job.setJarByClass(HRM.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setJobName(HRM.class.getCanonicalName());

        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        TextOutputFormat.setOutputPath(job, new Path(output));
        FileInputFormat.addInputPath(job, new Path(input));

        MultipleOutputs.addNamedOutput(job, TERMs, TextOutputFormat.class,
                Text.class, IntWritable.class);
        MultipleOutputs.addNamedOutput(job, DOCs, TextOutputFormat.class,
        Text.class, IntWritable.class);
        MultipleOutputs.addNamedOutput(job, CLICKs, TextOutputFormat.class,
        Text.class, IntWritable.class);

        job.setMapperClass(HRMMapper.class);
        job.setReducerClass(HRMReducer.class);


        //job.setNumReduceTasks(11);
        return job;
    }

    @Override
    public int run(final String[] args) throws Exception {
        
        final FileSystem fs = FileSystem.get(getConf());
        if (fs.exists(new Path(args[1]))) {
            fs.delete(new Path(args[1]), true);
        }
        final Job job = getJobConf(args[0], args[1]);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    static public void main(final String[] args) throws Exception {
        final int ret = ToolRunner.run(new HRM(), args);
        System.exit(ret);
    }
}