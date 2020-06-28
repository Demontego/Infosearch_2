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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;




public class sDBN extends Configured implements Tool {
    
    static private Map<String, Integer> get_url(final Mapper.Context context, final Path pt) {
        final Map<String, Integer> map = new HashMap<>();
        try {
            final FileSystem fs = FileSystem.get(context.getConfiguration());
            final BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fs.open(pt)));
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                final String[] split = line.split("\t");
                String s = split[1].charAt(split[1].length()-1)=='/' ? split[1].substring(0, split[1].length()-1) : split[1];
                map.put(s, Integer.parseInt(split[0]));
            }
        } catch (final Exception e) {
            e.printStackTrace();
        }

        return map;
    }

    static private Map<Integer, String> get_queries(final Mapper.Context context, final Path pt , Map<String, String> inv_q) {
        final Map<Integer, String> map = new HashMap<>();
        try {
            final FileSystem fs = FileSystem.get(context.getConfiguration());
            final BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fs.open(pt)));
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                final String[] split = line.split("\t");
                map.put(Integer.parseInt(split[0]), split[1].trim());
                if (inv_q!=null)
                    inv_q.put(split[1].trim(), split[0]);
            }
        } catch (final IOException e) {
            e.printStackTrace();
        }

        return map;
    }


    public static class sDBNMapper extends Mapper<LongWritable, Text, Text, Text> {
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
                    if(QD){
                        if (ids.containsKey(record.shownLinks.get(i))){//&inv_q.containsKey(record.query)) {
                            if(record.clickedLinks.contains(record.shownLinks.get(i))){
                                if(record.clickedLinks.indexOf(record.shownLinks.get(i))==record.clickedLinks.size()-1){
                                    String res = "1|1";
                                    context.write(new Text(record.query+"\t"+String.valueOf(ids.get(record.shownLinks.get(i)))), new Text(res));
                                }else{
                                    String res = "0|1";
                                    context.write(new Text(record.query+"\t"+String.valueOf(ids.get(record.shownLinks.get(i)))), new Text(res));
                                }
                            }else{
                                if(i<record.shownLinks.indexOf(record.clickedLinks.get(record.clickedLinks.size()-1))){
                                    String res = "0|0";
                                    context.write(new Text(record.query+"\t"+String.valueOf(ids.get(record.shownLinks.get(i)))), new Text(res));
                                }
                            }
                    }
                }else{
                    if (ids.containsKey(record.shownLinks.get(i))) {
                        if(record.clickedLinks.contains(record.shownLinks.get(i))){
                            if(record.clickedLinks.indexOf(record.shownLinks.get(i))==record.clickedLinks.size()-1){
                                String res = "1|1";
                                context.write(new Text(String.valueOf(ids.get(record.shownLinks.get(i)))), new Text(res));
                            }else{
                                String res = "0|1";
                                context.write(new Text(String.valueOf(ids.get(record.shownLinks.get(i)))), new Text(res));
                            }
                        }else{
                            if(i<record.shownLinks.indexOf(record.clickedLinks.get(record.clickedLinks.size()-1))){
                                String res = "0|0";
                                context.write(new Text(String.valueOf(ids.get(record.shownLinks.get(i)))), new Text(res));
                            }
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

    public static class sDBNReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(final Text key, final Iterable<Text> nums, final Context context)
                throws IOException, InterruptedException {
            
            int s_d = 0;
            int s_n = 0;
            int a_d = 0;
            int a_n = 0;
            for(Text val:nums){
                String[] split = val.toString().split("\\|");
                s_n += Integer.valueOf(split[0]);
                a_n += Integer.valueOf(split[1]);
                s_d += Integer.valueOf(split[1]);
                a_d++;
            }
            Double A = (double)(a_n+0.1)/(a_d+0.1+0.1);
            Double S = (double)(s_n+0.1)/(s_d+0.1+0.1);
            Double R= A*S;
            String s = String.valueOf(A)+"\t"+String.valueOf(S)+'\t'+String.valueOf(R);
            context.write(key, new Text(s));
        }
    }

    private Job getJobConf(final String input, final String output) throws IOException {

        final Job job = Job.getInstance(getConf());

        job.setJarByClass(sDBN.class);
        job.setInputFormatClass(TextInputFormat.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setMapperClass(sDBNMapper.class);
        job.setReducerClass(sDBNReducer.class);
        job.setNumReduceTasks(11);
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
        final int ret = ToolRunner.run(new sDBN(), args);
        System.exit(ret);
    }
}