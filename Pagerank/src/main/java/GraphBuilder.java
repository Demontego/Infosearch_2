import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.*;
import java.util.*;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;


class LinksExtractor {
    private Map<String, Integer> ids;
    Integer current_id = -1;
    Integer max_id_url = 564549;

    LinksExtractor(JobContext context) {
        ids = get_ids(context);
    }


    private Map<String, Integer> get_ids(JobContext context) {
        Map<String, Integer> map = new HashMap<>();

        try {
            //Path pt=new Path("hdfs:/data/infopoisk/hits_pagerank/urls.txt");//Location of file in HDFS
            Path pt=new Path("C:/Users/vkrin/OneDrive - C/techno3/infosearch/Pagerank/urls.txt");
            FileSystem fs = FileSystem.get(context.getConfiguration());
            BufferedReader bufferedReader =new BufferedReader(new InputStreamReader(fs.open(pt)));
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                String[] split = line.split("\t");
                map.put(split[1], Integer.parseInt(split[0]));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return map;
    }

    public ArrayList<Integer> get_links(String compr_html) {
        String html = unzip(compr_html);
        Document doc = Jsoup.parse(html);
        Elements links = doc.select("a[href]");

        ArrayList<Integer> cur_links = new ArrayList<>();
        for (Element link : links) {
            String ext_link = link.attr("href");
            if (ext_link.length() <= 1)
                continue;
            if (ext_link.charAt(0) == '/') {
                ext_link = "http://lenta.ru" + ext_link;
            }
            if (ids.containsKey(ext_link)) {
                cur_links.add(ids.get(ext_link));
            }else{
                //ids.put(ext_link,max_id_url++);
              //  cur_links.add(ids.get(ext_link));
            }
        }
        return cur_links;
    }

    private static String unzip(String encoded) {
        byte[] compressed;
        try {
            compressed = Base64.getMimeDecoder().decode(encoded);
        } catch (Exception e) {
            throw new RuntimeException("Failed to unzip content", e);
        }

        if ((compressed == null) || (compressed.length == 0)) {
            throw new IllegalArgumentException("Cannot unzip null or empty bytes");
        }
        try {
            Inflater decompressor  = new Inflater();
            decompressor.setInput(compressed, 0, compressed.length);
            byte[] result = new byte[compressed.length*10];
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            while (!decompressor.needsInput() & !decompressor.finished()){
                int len = decompressor.inflate(result);
                baos.write(result, 0, len);
            }
            decompressor.end();
            return baos.toString(StandardCharsets.UTF_8.name());
        } catch (DataFormatException | UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return "";
    }
}

public class GraphBuilder extends Configured implements Tool {
    public static class GraphBuilderMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {
        LinksExtractor linksExtractor;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            linksExtractor = new LinksExtractor(context);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] id_html=value.toString().split("\t");
            ArrayList<Integer> links = linksExtractor.get_links(id_html[1]);
            LongWritable k = new LongWritable(Integer.parseInt(id_html[0]));
            for (int link : links) {
                LongWritable v = new LongWritable(link);
                context.write(k, v);
                //if(link>564548)
                  //  context.write(v, k);
            }
        }
    }

    public static class GraphBuilderReducer extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
        @Override
        protected void reduce(LongWritable key, Iterable<LongWritable> nums, Context context) throws IOException, InterruptedException {

            for (LongWritable val : nums) {
                context.write(key, val);
            }
        }
    }

    private Job getJobConf(String input, String output) throws IOException {
        Job job = Job.getInstance(getConf());

        job.setJarByClass(GraphBuilder.class);
        job.setJobName(GraphBuilder.class.getCanonicalName());

        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setMapOutputKeyClass(LongWritable.class);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setMapperClass(GraphBuilderMapper.class);
        job.setReducerClass(GraphBuilderReducer.class);

//        job.setNumReduceTasks(10);

        return job;
    }

    @Override
    public int run(String[] args) throws Exception {
        final FileSystem fs = FileSystem.get(getConf());
        if (fs.exists(new Path(args[1]))) {
            fs.delete(new Path(args[1]), true);
        }
        Job job = getJobConf(args[0], args[1]);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    static public void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new GraphBuilder(), args);

        System.exit(ret);
    }
}
