import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.List;

public class QRecord implements Writable {
    private static String DELIMETER = "\t";
    private static String LIST_DELIMETER = ",http";
    private static String QUERY_GEO_DELIMETER = "@";
    private static String TIMES_DELIMETER = ",";

    public String query = "";
    public int queryGeo = -1;
    public List<String> shownLinks = new ArrayList<>();
    public List<String> clickedLinks = new ArrayList<>();
    public List<Long> timestamps = new ArrayList<>();
    public Map<String, Long> time_map = new HashMap<>();
    public boolean hasTimeMap;
    public boolean host;

    public QRecord(final Boolean host) {
        this.host=host;
    }

    public QRecord(final String query, final int queryGeo, final List<String> shownLinks,
            final List<String> clickedLinks) {
        this.query = query;
        this.queryGeo = queryGeo;
        this.shownLinks = shownLinks;
        this.clickedLinks = clickedLinks;
    }

    public QRecord(final String query, final int queryGeo, final List<String> shownLinks,
            final List<String> clickedLinks, final List<Long> timestamps) {
        this.query = query;
        this.queryGeo = queryGeo;
        this.shownLinks = shownLinks;
        this.clickedLinks = clickedLinks;
        this.timestamps = timestamps;
    }

    public QRecord(final String data_string) throws URISyntaxException {
        this.parseString(data_string);
    }

    public void parseString(final String in) throws URISyntaxException {
        int idx = 0;
        final String[] args = in.split(DELIMETER);

        final String[] query_args = args[idx++].split(QUERY_GEO_DELIMETER);

        query = prepareQuery(query_args[0]);
        queryGeo = Integer.parseInt(query_args[1]);

        shownLinks = new ArrayList<>();
        final String[] shownLinks_args = args[idx++].split(LIST_DELIMETER);
        for (final String url : shownLinks_args) {
            if (url.startsWith("://") | url.startsWith("http"))
                if (host)
                    shownLinks.add(prepareHost(url));
                else
                    shownLinks.add(prepareUrl(url));
        }

        clickedLinks = new ArrayList<>();
        final String[] clickedLinks_args = args[idx++].split(LIST_DELIMETER);
        for (final String url : clickedLinks_args) {
            if (url.startsWith("://") | url.startsWith("http"))
                if (host)
                    clickedLinks.add(prepareHost(url));
                else
                    clickedLinks.add(prepareUrl(url));
        }

        timestamps = new ArrayList<>();
        final String[] timestamps_args = args[idx++].split(TIMES_DELIMETER);
        for (final String timestamp : timestamps_args) {
            timestamps.add(Long.parseLong(timestamp));
        }
        if (timestamps.size() == clickedLinks.size()) {
            this.hasTimeMap = true;
            for (int i = 0; i < clickedLinks.size() - 1; ++i) {
                final Long time = (timestamps.get(i + 1) - timestamps.get(i)) / 1000;
                time_map.put(clickedLinks.get(i), time);
            }
            time_map.put(clickedLinks.get(clickedLinks.size() - 1), (long) 352);
        }
        query = prepareQuery(query);
    }

    public static String prepareQuery(final String query) {
        return query.trim();
    }

    public static String prepareUrl(final String url) {
        String s = url.startsWith("://") ? url.substring(3) : url;
        s = s.startsWith("http://") ? s.substring(7) : s;
        if (s.length() == 0)
            return "";
        s = s.charAt(s.length() - 1) == '/' ? s.substring(0, s.length() - 1) : s;
        return s.trim();
    }

    public static String prepareHost(final String url) throws URISyntaxException {
        final URI uriBase = new URI("http" + url);
        return uriBase.getHost();
    }

    @Override
    public void write(final DataOutput out) throws IOException {
        out.writeUTF(query);
        out.writeInt(queryGeo);

        out.writeInt(shownLinks.size());
        for (int i = 0; i < shownLinks.size(); i++) {
            out.writeUTF(shownLinks.get(i));
        }

        out.writeInt(clickedLinks.size());
        for (int i = 0; i < clickedLinks.size(); i++) {
            out.writeUTF(clickedLinks.get(i));
        }

        out.writeInt(timestamps.size());
        for (int i = 0; i < timestamps.size(); i++) {
            out.writeLong(timestamps.get(i));
        }
    }

    @Override
    public void readFields(final DataInput in) throws IOException {
        query = in.readUTF();
        queryGeo = in.readInt();

        shownLinks = new ArrayList<>();
        final int shownLinksSize = in.readInt();
        for (int i = 0; i < shownLinksSize; i++) {
            shownLinks.add(in.readUTF());
        }

        clickedLinks = new ArrayList<>();
        final int clickedLinksSize = in.readInt();
        for (int i = 0; i < clickedLinksSize; i++) {
            clickedLinks.add(in.readUTF());
        }

        timestamps = new ArrayList<>();
        final int timestampsSize = in.readInt();
        for (int i =0; i< timestampsSize; i++)
        {
            timestamps.add(in.readLong());
        }
    }
}