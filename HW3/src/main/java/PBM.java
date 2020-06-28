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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PBM extends Configured implements Tool{
    
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

    public static class PBMMapper extends Mapper<LongWritable, Text, Text, Text> {
        static final IntWritable one = new IntWritable(1);
        static final IntWritable null_0 = new IntWritable(0);
        static Map<String, Integer> ids;
        static Map<Integer, String> queries;
        static Map<Integer, Integer> query_url = new HashMap<>();
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
                        if (ids.containsKey(record.shownLinks.get(i))) {
                            if (record.clickedLinks.contains(record.shownLinks.get(i))) {
                                if(record.clickedLinks.indexOf(record.shownLinks.get(i))==0){
                                    final String res = record.query+"|1|1|0|"+ String.valueOf(i)+"|1";
                                    context.write(new Text(String.valueOf(ids.get(record.shownLinks.get(i)))), new Text(res));
                                }else{
                                    if (record.clickedLinks.indexOf(record.shownLinks.get(i))==record.clickedLinks.size()-1){
                                    final String res = record.query+"|1|0|1|"+ String.valueOf(i)+"|1";
                                    context.write(new Text(String.valueOf(ids.get(record.shownLinks.get(i)))), new Text(res));
                                    }else{
                                        final String res =record.query+"|1|0|0|"+ String.valueOf(i)+"|1";
                                        context.write(new Text(String.valueOf(ids.get(record.shownLinks.get(i)))), new Text(res));
                                    }
                                }
                            } else {
                                for(int j= 0; j < record.clickedLinks.size();++j){
                                    if(record.shownLinks.indexOf(record.clickedLinks.get(j))<i){
                                        final String res = record.query+"|0|0|0|"+String.valueOf(i)+"|0";
                                        context.write(new Text(String.valueOf(ids.get(record.shownLinks.get(i)))), new Text(res));
                                        return;
                                    }
                                    if(record.shownLinks.indexOf(record.clickedLinks.get(j))<i)
                                        break;
                                }
                                final String res = record.query+"|0|0|0|"+String.valueOf(i)+"|1";
                                context.write(new Text(String.valueOf(ids.get(record.shownLinks.get(i)))), new Text(res));
                            }
                        }
                    }
            } catch (final Exception e) {
                e.printStackTrace();
                return;
            }
        }

    }

    public static class PBMReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(final Text key, final Iterable<Text> nums, final Context context)
                throws IOException, InterruptedException {
            Double[] coef = new Double[]{0.41,0.16,0.105,0.08,0.06,0.05,0.35,0.3,0.25,0.2};
            int[] click_pos= new int[12];
            int[] shows_pos= new int[12];
            int num_first_click=0;
            int num_shows_before_first=0;
            int click_pbm = 0;
            int cascade = 0;
            int click_cm=0;
            double show = 0.;
            for (final Text val : nums) {
                final List<String> split = Arrays.asList(val.toString().split("\\|"));
                int pos=Integer.valueOf(split.get(4));
                int click = Integer.valueOf(split.get(1));
                click_cm+=click;
                int first_click = Integer.valueOf(split.get(2));
                int last_click = Integer.valueOf(split.get(3));
                cascade += Integer.parseInt(split.get(5));
                if(pos<10){
                    click_pbm+=click;
                    show+=coef[pos];
                }
                if(pos<12){
                    click_pos[pos]+=click;
                    shows_pos[pos]++;
                }
                if(click==1){
                    if(first_click==1){
                        num_first_click++;
                        num_shows_before_first+=pos;
                    }
                }
            }
            String s = "";
            double click_pos_double;
            double E_click= 0;
            for (int i=0;i<12;i++){
                click_pos_double = (double)click_pos[i]/(shows_pos[i]+0.000001);
                s+=String.valueOf(click_pos_double)+"\t";
                E_click+=(i+1)*click_pos_double;
            }
            String PBM = String.valueOf((double)click_pbm/(show+0.000001))+"\t";
            String CM = String.valueOf((double)click_cm/(cascade+0.0001));
            String Mean_shows=String.valueOf((double)num_shows_before_first/(num_first_click+0.000001))+"\t";
            s+=E_click+"\t"+Mean_shows+PBM+CM;
            context.write(key, new Text(s));
        }
    }

    private Job getJobConf(final String input, final String output) throws IOException {

        final Job job = Job.getInstance(getConf());

        job.setJarByClass(PBM.class);
        job.setInputFormatClass(TextInputFormat.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setMapperClass(PBMMapper.class);
        job.setReducerClass(PBMReducer.class);
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
        final int ret = ToolRunner.run(new PBM(), args);
        System.exit(ret);
    }
}