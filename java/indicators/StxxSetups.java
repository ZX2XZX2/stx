package indicators;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Calendar;
import java.util.GregorianCalendar;

import core.StxCal;
import core.StxData;
import indicators.StxSetups;


public class StxxSetups {
    private StxData<StxSetups> recs;
    private String stk;
    
    public StxxSetups(String stk) {
        recs = new StxData<StxSetups>();
        this.stk = stk;
        BufferedReader br = null;
        try {
            String line;
            String fname = String.format("/home/cma/setups/%s.csv", stk);
            br = new BufferedReader(new FileReader(fname));
            while ((line = br.readLine()) != null) {
                String[] tokens = line.split(";");
                String dt = tokens[0].trim();
                // System.err.printf("line = %s, len(tokens) = %d, dt = %s\n",
                //                line, tokens.length, dt);
                String sss = (tokens.length > 1) ? tokens[1].trim(): "";
                recs.add(new StxSetups(dt, sss));
            }
        } catch (Exception e) {
            // System.err.println(e.toString());
            // e.printStackTrace();
        } finally {
            try {
                if (br != null)
                    br.close();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }
    public String getSetups(String dt) {
        if(recs.size() == 0)
            return "";
        int ix = recs.find(dt, 0);
        StringBuffer sb = new StringBuffer();
        if(ix > 1)
            sb.append("T-2: ").append(recs.get(ix - 2).setups).append("\n");
        if(ix > 0)
            sb.append("T-1: ").append(recs.get(ix - 1).setups).append("\n");
        if(ix >= 0)
            sb.append("  T: ").append(recs.get(ix).setups).append("\n");
        return  sb.toString();
    }

    public String getStk() { return stk; }

    public static void main( String [] args) throws Exception {
        Calendar c = new GregorianCalendar();
        new StxCal(c.get(Calendar.YEAR) + 1);
        String dt = args[1], stk = args[0];
        StxxSetups sss = new StxxSetups(stk);
        System.err.printf("dt = %s, stk = %s\n", dt, stk);
        String res = sss.getSetups(dt);
        System.err.println(res);
    }    
}
