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
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;




public class CTR extends Configured implements Tool {
    public static final String OnlyDOCs = "OnlyDocs";
    public static final String QDOCs = "QDOCs";

    public static int levenstain(@Nonnull final String str1, @Nonnull final String str2) {
        final int[] Di_1 = new int[str2.length() + 1];
        final int[] Di = new int[str2.length() + 1];

        for (int j = 0; j <= str2.length(); j++) {
            Di[j] = j; // (i == 0)
        }

        for (int i = 1; i <= str1.length(); i++) {
            System.arraycopy(Di, 0, Di_1, 0, Di_1.length);

            Di[0] = i; // (j == 0)
            for (int j = 1; j <= str2.length(); j++) {
                final int cost = (str1.charAt(i - 1) != str2.charAt(j - 1)) ? 1 : 0;
                Di[j] = min(Di_1[j] + 1, Di[j - 1] + 1, Di_1[j - 1] + cost);
            }
        }

        return Di[Di.length - 1];
    }

    private static int min(final int n1, final int n2, final int n3) {
        return Math.min(Math.min(n1, n2), n3);
    }

    static private Map<String, Integer> get_url(final Mapper.Context context, final Path pt, boolean host) {
        final Map<String, Integer> map = new HashMap<>();
        try {
            final FileSystem fs = FileSystem.get(context.getConfiguration());
            final BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fs.open(pt)));
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                final String[] split = line.split("\t");
                if (host) {
                    final URI url = new URI("http://" + split[1]);
                    map.put(url.getHost(), Integer.parseInt(split[0]));
                } else {
                    final String s = split[1].charAt(split[1].length() - 1) == '/'
                            ? split[1].substring(0, split[1].length() - 1)
                            : split[1];
                    map.put(s, Integer.parseInt(split[0]));
                }
            }
        } catch (final Exception e) {
            e.printStackTrace();
        }

        return map;
    }

    static String similarity(final String q1, final String q2) {
        double lev = 0;
        int sum_q1 = 0;
        try {
            for (final String s1 : q1.split("\\ ")) {
                int tmp = 0;
                int sum_q2 = 0;
                for (final String s2 : q2.split("\\ ")) {
                    tmp += levenstain(s1, s2);
                    sum_q2++;
                }
                lev += (double) tmp / sum_q2;
                sum_q1++;
            }
        } catch (final NullPointerException e) {
            // e.printStackTrace();
            return String.valueOf(0);
        }
        return String.valueOf(sum_q1 / lev);
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

    public static class CTRMapper extends Mapper<LongWritable, Text, Text, Text> {
        static final IntWritable one = new IntWritable(1);
        static final IntWritable null_0 = new IntWritable(0);
        static Map<String, Integer> ids;
        static Map<Integer, String> queries;
        static Map<Integer, Integer> query_url = new HashMap<>();
        final boolean host = true;
        Map<String, String> inv_q = new HashMap<>();

        @Override
        protected void setup(final Context context) throws IOException, InterruptedException {
            super.setup(context);
            final Path urls = new Path("check/url.data/url.data");
            final Path q = new Path("check/queries.tsv");
            final Path train = new Path("check/train.marks.tsv/train.marks.tsv");
            final Path sample = new Path("check/sample.csv/sample.csv");
            ids = get_url(context, urls, host);
            queries = get_queries(context, q, inv_q);
            try {
                final FileSystem fs = FileSystem.get(context.getConfiguration());
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fs.open(train)));
                String line;
                while ((line = bufferedReader.readLine()) != null) {
                    final String[] split = line.split("\t");
                    query_url.put(Integer.valueOf(split[1]), Integer.valueOf(split[0]));
                }
                bufferedReader.close();
                bufferedReader = new BufferedReader(new InputStreamReader(fs.open(sample)));
                while ((line = bufferedReader.readLine()) != null) {
                    final String[] split = line.split("\\,");
                    query_url.put(Integer.valueOf(split[1]), Integer.valueOf(split[0]));
                }
                bufferedReader.close();
            } catch (final IOException e) {
                e.printStackTrace();
            }

        }


        @Override
        protected void map(final LongWritable key, final Text value, final Context context)
                throws IOException, InterruptedException {
            try {
                final QRecord record = new QRecord(host);
                record.parseString(value.toString());
                for(int i=0;i<record.shownLinks.size();++i){
                    if (ids.containsKey(record.shownLinks.get(i))) {
                        if (record.clickedLinks.contains(record.shownLinks.get(i))) {
                            if (record.clickedLinks.indexOf(record.shownLinks.get(i)) == 0) {
                                if (record.hasTimeMap) {
                                    final String res = record.query+"|1|"
                                            + similarity(
                                                    queries.get(query_url.get(ids.get(record.shownLinks.get(i)))),
                                                    record.query)
                                            + "|" + String.valueOf(record.time_map.get(record.shownLinks.get(i)))
                                            + "|1|0|" + String.valueOf(i) + "|1";
                                    context.write(new Text(String.valueOf(ids.get(record.shownLinks.get(i)))),
                                            new Text(res));
                                } else {
                                    final String res =  record.query+"|1|" + similarity(
                                            queries.get(query_url.get(ids.get(record.shownLinks.get(i)))),
                                            record.query) + "|352|1|0|" + String.valueOf(i) + "|1";
                                    context.write(new Text(String.valueOf(ids.get(record.shownLinks.get(i)))),
                                            new Text(res));
                                }
                            } else {
                                if (record.clickedLinks
                                        .indexOf(record.shownLinks.get(i)) == record.clickedLinks.size() - 1) {
                                    final String res = record.query+"|1|"
                                            + similarity(
                                                    queries.get(query_url.get(ids.get(record.shownLinks.get(i)))),
                                                    record.query)
                                            + "|352|0|1|" + String.valueOf(i) + "|"
                                            + String.valueOf(record.clickedLinks.size() - 1);
                                    context.write(new Text(String.valueOf(ids.get(record.shownLinks.get(i)))),
                                            new Text(res));
                                } else {
                                    if (record.hasTimeMap) {
                                        final String res =  record.query+"|1|"
                                                + similarity(
                                                        queries.get(
                                                                query_url.get(ids.get(record.shownLinks.get(i)))),
                                                        record.query)
                                                + "|"
                                                + String.valueOf(record.time_map.get(record.shownLinks.get(i)))
                                                + "|0|0|" + String.valueOf(i) + "|" + String.valueOf(
                                                        record.clickedLinks.indexOf(record.shownLinks.get(i)));
                                        context.write(new Text(String.valueOf(ids.get(record.shownLinks.get(i)))),
                                                new Text(res));
                                    } else {
                                        final String res =  record.query+"|1|"
                                                + similarity(
                                                        queries.get(
                                                                query_url.get(ids.get(record.shownLinks.get(i)))),
                                                        record.query)
                                                + "|352|0|0|" + String.valueOf(i) + "|" + String.valueOf(
                                                        record.clickedLinks.indexOf(record.shownLinks.get(i)));
                                        context.write(new Text(String.valueOf(ids.get(record.shownLinks.get(i)))),
                                                new Text(res));
                                    }
                                }
                            }
                        } else {
                            final String res =  record.query+"|0|0|0|0|0|" + String.valueOf(i) + "|0";
                            context.write(new Text(String.valueOf(ids.get(record.shownLinks.get(i)))),
                                    new Text(res));
                        }
                    }
                }

            } catch (final Exception e) {
                e.printStackTrace();
                return;
            }
        }

    }

    public static class CTRReducer extends Reducer<Text, Text, Text, Text> {

        private MultipleOutputs<Text, Text> multipleOutputs;

        public void setup(final Reducer.Context context) {
            multipleOutputs = new MultipleOutputs(context);

        }
        @Override
        protected void reduce(final Text key, final Iterable<Text> nums, final Context context)
                throws IOException, InterruptedException {
            
            Map<String,Double[]> local_stats = new HashMap<>();
            Double sum_global_clicks = 0.;
            Double sim = 0.;
            Double sum_global_shows = 0.;
            Double view_time = 0.;
            Double first_click = 0.;
            Double last_click = 0.;
            Double poshow = 0.;
            Double posclick = 0.;
            Double clicks_before = 0.;
            Double doc_before = 0.;
            Double FirstClickPos = 0.;
            for (final Text val : nums) {
                final List<String> split = Arrays.asList(val.toString().split("\\|"));
                if(local_stats.containsKey(split.get(0))){
                    Double[] update = local_stats.get(split.get(0));
                    update[0] += Double.valueOf(split.get(1));
                    update[1] +=  Double.valueOf(split.get(2))*Integer.valueOf(split.get(1));
                    update[2] += 1.;
                    update[3] += Math.log1p(Double.valueOf(split.get(3)));
                    update[4] += Double.valueOf(split.get(4));
                    update[5] += Double.valueOf(split.get(5));
                    update[6] += Double.valueOf(split.get(6));
                    update[7] += Double.valueOf(split.get(7))+1.;
                    update[8] += Double.valueOf(split.get(7));
                    if(Double.valueOf(split.get(4))==1.)
                        update[9]+=1.;
                    if(Double.valueOf(split.get(5))==0. & Integer.valueOf(split.get(1))==1)
                        update[10]+=1.;
                    local_stats.put(split.get(0), update);
                }else{
                    Double[] stat =new Double[]{Double.valueOf(split.get(1)), Double.valueOf(split.get(2))*Integer.valueOf(split.get(1)),
                        1.,Math.log1p(Double.valueOf(split.get(3))),Double.valueOf(split.get(4)),
                        Double.valueOf(split.get(5)),Double.valueOf(split.get(6)),Double.valueOf(split.get(7))+1.,
                        Double.valueOf(split.get(7)),0.,0.};
                    if(stat[4]==1.)
                        stat[9]=1.;
                    if(stat[5]==0. & stat[0]==0.)
                        stat[10]=1.;
                    local_stats.put(split.get(0), stat);
                }
                sim +=  Double.valueOf(split.get(2))*Integer.valueOf(split.get(1));
                first_click += Double.valueOf(split.get(4));
                if (Integer.valueOf(split.get(4)) == 1.)
                    FirstClickPos+=1.;
                last_click += Double.valueOf(split.get(5));
                if (Double.valueOf(split.get(5)) == 0. & Integer.valueOf(split.get(1))== 1)
                    doc_before+=1.;
                poshow += Double.valueOf(split.get(6));
                posclick += Double.valueOf(split.get(7))+1.;
                sum_global_clicks += Integer.valueOf(split.get(1));
                clicks_before += (Double.valueOf(split.get(7)));
                try {
                    view_time += Math.log1p(Double.valueOf(split.get(3)));
                } catch (final Exception e) {
                    e.printStackTrace();
                }
                sum_global_shows += 1.;
            }
            for(Map.Entry<String, Double[]> entry : local_stats.entrySet()){

                final double AvgViewTime =  entry.getValue()[3] /  entry.getValue()[2];
                final Double GlobalCTR =  entry.getValue()[0] / entry.getValue()[2];
                final Double QCTR = entry.getValue()[1] / entry.getValue()[2];
                final Double FirstCTR = entry.getValue()[4] / entry.getValue()[2];
                final Double LastCTR = entry.getValue()[5] / entry.getValue()[2];
                final Double PosDoc =entry.getValue()[6] / entry.getValue()[2];
                final Double LastProb = entry.getValue()[7] / (entry.getValue()[0]+0.001);
                final double AvgNumBefore = entry.getValue()[8] / (entry.getValue()[0]+0.001);
                final double BeforeProb = entry.getValue()[9] / (entry.getValue()[0]+0.001);
                final double AvgFirstPos = entry.getValue()[10] / (entry.getValue()[0]+0.001);
                final double NoCLick = 1. - entry.getValue()[0] / entry.getValue()[2];
                final Double PosClick=entry.getValue()[7]/(entry.getValue()[0]+0.001);
                final String s =  String.valueOf(GlobalCTR)+"\t"+String.valueOf(QCTR)+"\t"+String.valueOf(AvgViewTime)
                            +"\t"+String.valueOf(FirstCTR)+"\t"+String.valueOf(LastCTR)+"\t"+String.valueOf(PosDoc)+
                            "\t"+String.valueOf(PosClick)+"\t"+String.valueOf(LastProb)+"\t"+String.valueOf(AvgNumBefore)+"\t"+String.valueOf(BeforeProb)
                            + "\t"+String.valueOf(AvgFirstPos)+ "\t"+String.valueOf(NoCLick);
                multipleOutputs.write(new Text(key+"\t"+entry.getKey()), new Text(s), QDOCs+"/part");
            }
            final double AvgViewTime = view_time / sum_global_shows;
            final Double GlobalCTR = sum_global_clicks / (sum_global_shows);
            final Double QCTR = sim / sum_global_shows;
            final Double FirstCTR = first_click / sum_global_shows;
            final Double LastCTR = last_click / sum_global_shows;
            final Double PosDoc =  poshow / sum_global_shows;
            final Double LastProb = last_click /(sum_global_clicks+0.001);
            final double AvgNumBefore = clicks_before /(sum_global_clicks+0.001);
            final double BeforeProb = doc_before /(sum_global_clicks+0.001);
            final double AvgFirstPos = FirstClickPos / (sum_global_clicks+0.001);
            final double NoCLick = 1. - sum_global_clicks / sum_global_shows;
            final Double PosClick=posclick/(sum_global_clicks+0.001);
            final String s =  String.valueOf(GlobalCTR)+"\t"+String.valueOf(QCTR)+"\t"+String.valueOf(AvgViewTime)
                        +"\t"+String.valueOf(FirstCTR)+"\t"+String.valueOf(LastCTR)+"\t"+String.valueOf(PosDoc)+
                        "\t"+String.valueOf(PosClick)+"\t"+String.valueOf(LastProb)+"\t"+String.valueOf(AvgNumBefore)+"\t"+String.valueOf(BeforeProb)
                        + "\t"+String.valueOf(AvgFirstPos)+ "\t"+String.valueOf(NoCLick);
            multipleOutputs.write(new Text(key), new Text(s), OnlyDOCs+"/part");
        }
        
        public void cleanup(final Context context) throws IOException, InterruptedException {
            super.cleanup(context);
            multipleOutputs.close();
        }
    }

    private Job getJobConf(final String input, final String output) throws IOException {

        final Job job = Job.getInstance(getConf());

        job.setJarByClass(CTR.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setJobName(CTR.class.getCanonicalName());

        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        TextOutputFormat.setOutputPath(job, new Path(output));
        FileInputFormat.addInputPath(job, new Path(input));

        MultipleOutputs.addNamedOutput(job, OnlyDOCs, TextOutputFormat.class,Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, QDOCs, TextOutputFormat.class,Text.class, Text.class);

        job.setMapperClass(CTRMapper.class);
        job.setReducerClass(CTRReducer.class);
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
        final int ret = ToolRunner.run(new CTR(), args);
        System.exit(ret);
    }
}
