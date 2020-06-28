import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;





public class HelperPython extends Configured implements Tool {

    

    public static class HPMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void setup(final Context context) throws IOException, InterruptedException {
            super.setup(context);


        }

        @Override
        protected void map(final LongWritable key, final Text value, final Context context)
                throws IOException, InterruptedException {
            try {
                int ind = value.toString().indexOf("\t");
                int first=Integer.parseInt(value.toString().substring(0,ind));
                String val= value.toString().substring(ind+1);
                ind = val.indexOf("\t");
                int second = Integer.valueOf(val.substring(0, ind));
                String res = val.substring(ind+1);
                String[] splits= res.split("\t");
                if (second==-1){
                    context.write(new Text(String.valueOf(splits[0])), new Text("-1\t"+res));
                }else{
                context.write(new Text(String.valueOf(second)),new Text(String.valueOf(first)+"\t"+res));
                }
               
            } catch (final Exception e) {
                e.printStackTrace();
                return;
            }
        }

    }

    public static class HPReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(final Text key, final Iterable<Text> nums, final Context context)
                throws IOException, InterruptedException {
            Map<Integer,Map<String,Double>> coefs = new HashMap<>();
            Map<String,List<Double>> vectors =  new HashMap<>();
            List<Integer> ids = new ArrayList<>();
            for (Text val: nums){
                try{
                    int ind = val.toString().indexOf("\t");
                    int id_q=Integer.parseInt(val.toString().substring(0,ind));
                    String substr = val.toString().substring(ind+1);
                    if(id_q!=-1){
                        ids.add(id_q);
                        Map<String,Double> coef = new HashMap<>();
                        String[] splits = substr.split("\t");
                        for(String split: splits){
                            String[] s = split.split("\\:");
                            coef.put(s[0], Double.parseDouble(s[1]));
                        }
                        coefs.put(id_q,coef);
                    }else{
                        List<Double> vector= new ArrayList<Double>();
                        int ind_q = substr.indexOf("\t");
                        String q = substr.substring(0, ind_q);
                        String[] splits = substr.substring(ind_q+1).split("\t");
                        for(String split: splits){
                            vector.add(Double.valueOf(split));
                        }
                        vectors.put(q,vector);
                    }
                }catch(Exception e)
                {
                    //e.printStackTrace();
                    continue;
                }
            }
            for (int id_q: ids ){
                Double[] res = new Double[3];
                for(int i=0;i<3;++i){
                    res[i]=0.;
                    for(Map.Entry<String, Double> iter : coefs.get(id_q).entrySet()){
                        try{
                        res[i]+=vectors.get(iter.getKey()).get(i)*iter.getValue();}
                        catch(Exception e){
                            //e.printStackTrace();
                            continue;
                        }
                    }
                }
                String result= "";
                for(Double r: res){
                    result+=r/coefs.get(id_q).size()+"\t";
                }
                context.write(new Text(String.valueOf(id_q)+"\t"+key), new Text(result));
            }
        }
    }

    private Job getJobConf(final String input, final String output) throws IOException {

        final Job job = Job.getInstance(getConf());

        job.setJarByClass(CTR.class);
        job.setInputFormatClass(TextInputFormat.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setMapperClass(HPMapper.class);
        job.setReducerClass(HPReducer.class);
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
        final int ret = ToolRunner.run(new HelperPython(), args);
        System.exit(ret);
    }
}
