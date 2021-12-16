package indicators;

import static java.lang.Math.*;
import core.StxCal;
import core.StxRec;
import core.StxTS;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class StxADX {

    private StxTS<StxRec> data;
    private boolean initialized;
    private int adxd;
    private float tpdm, tmdm, ttr, pdi, mdi, tadx;


    public StxADX(StxTS<StxRec> data) {
        this.data = data;
        init(14);
    }

    public StxADX(StxTS<StxRec> data, int adxd) {
        this.data = data;
        init(adxd);
    }
    
    private void init(int adxd) {
        this.adxd = adxd;
        tpdm = 0;
        tmdm = 0;
        ttr = 0;
        pdi= 0;
        mdi= 0;
        tadx= 0;
        initialized = false;
    }   
        
    public int adxi(int ix) { return (int) adx(ix); }
    
    public HashMap<String, Float> daily_calc(int ix) {
        HashMap<String, Float> res = new HashMap<String, Float>();
        StxRec sr = data.get(ix), sr_1 = data.get(ix - 1);
        /** The indicators to compute */
        float pdm= 0, mdm= 0, tr= 0;
        /** compute +DM and -DM */
        pdm = sr.h - sr_1.h;
        mdm = sr_1.l - sr.l;
        if((pdm < 0) || (pdm < mdm))
            pdm = 0;
        if((mdm < 0) || (mdm < pdm))
            mdm = 0;
        /** Compute True Range */
        tr = sr.trueRange(sr_1);
        res.put("pdm", pdm);
        res.put("mdm", mdm);
        res.put("tr", tr);
        return res;
    }
    
    public float next_dx(HashMap<String, Float> daily) {
        /** smooth PDM, MDM, and True Range; calculate +DI/-DI/DX */
        float ypdm = tpdm, ymdm = tmdm, ytr = ttr; 
        tpdm = ypdm - ypdm / adxd + daily.get("pdm");
        tmdm = ymdm - ymdm / adxd + daily.get("mdm");
        ttr = ytr - ytr / adxd + daily.get("tr");
        pdi = 100 * tpdm / ttr;
        mdi = 100 * tmdm / ttr;
        return 100 * abs(pdi - mdi) / (pdi + mdi);
    }
    
    public float adx(int ix) {
        float dx= 0, ydx= 0, tdx= 0, yadx= 0;
        if(initialized) {
            HashMap<String, Float> daily = daily_calc(ix);
            dx = next_dx(daily);
            yadx = tadx;
            tadx = ((adxd - 1) * yadx + dx) / adxd;
            return tadx * ((pdi > mdi)? 1: -1);
        }
        initialized = true;
        for(int dx_idx = 0; dx_idx <= ix; dx_idx++) {
            if(dx_idx == 0) continue;
            HashMap<String, Float> daily = daily_calc(dx_idx);
            if( dx_idx <= adxd) { /** days 1-14: add up +DM, -DM, True Range */
                tpdm += daily.get("pdm");
                tmdm += daily.get("mdm");
                ttr += daily.get("tr");
            }
            if( dx_idx == adxd) { /** day 15: got +DI/-DI; compute first DX */
                pdi = 100 * tpdm / ttr;
                mdi = 100 * tmdm / ttr;
                tdx = 100 * abs(pdi - mdi) / (pdi + mdi);
            }
            if(dx_idx > adxd) {
                dx = next_dx(daily);
                if(dx_idx < 2 * adxd - 1)
                    /** days 15-27: start adding up DX */
                    tdx += dx;
                else if(dx_idx == 2 * adxd - 1) { 
                    /** day 27: compute first ADX 
                        (DX average) */
                    tdx += dx;
                    tadx = tdx/ adxd;
                }
                else { /** Smooth average ADX */
                    yadx = tadx;
                    tadx = ((adxd - 1) * yadx + dx) / adxd;
                }
            } /** end ( dx_idx > adxd)) */
//             System.err.printf( "%s,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f\n", sr.date, sr.h, sr.l, sr.c, tr, pdm, mdm, ttr, tpdm, tmdm, pdi, mdi, di_diff, di_sum, tdx, tadx);
        } /** end main for loop */
        return tadx * ((pdi > mdi)? 1: -1);
    }

    public static String usage() {
        return "java StxADX stk_name <-sd start_date> " +
            "<-ed end_date> <-d number of adx days>";
    }
    
    public static void main(String[] args) {
        String stk = null, sd = null, ed = null;
        int adxd = 14;
        int ix = 0, s_ix = 0, e_ix = 0;
        while (ix < args.length) {
            String arg = args[ix++];
            if(ix == 1)
                stk = arg;
            else if(arg.equals("-sd")) {
                if(ix < args.length)
                    sd = args[ix++];
                else
                    System.err.println("-sd requires a start date");
            } else if(arg.equals("-ed")) {
                if(ix < args.length)
                    ed = args[ix++];
                else
                    System.err.println("-ed requires an end date");
            } else if(arg.equals("-d")) {
                if(ix < args.length)
                    try {
                        adxd = Integer.parseInt(args[ix++]);
                    } catch(Exception ex) {
                        System.err.println("Failed to parse integer "+
                                           "for # of ADX days" +
                                           args[ix]);
                    }
                else
                   System.err.println("-d requires # of days");
            } else
                System.err.println("Unknown argument " + arg +
                                   "\nUsage: " + usage());
        }
        if(stk == null) {
            System.err.println("Missing mandatory argument stock name" +
                               "\nUsage: " + usage());
            System.exit(0);
        }
        StxTS<StxRec> ts = StxTS.loadEod(stk, sd, ed);
        StxADX adx = new StxADX(ts, adxd);
        if(sd == null && ed == null) {
            // set start and end dates as the beginning and end of the
            // last gap
            List<String> start_end = ts.getLastSeries();
            sd = start_end.get(0);
            ed = start_end.get(1);
        } else if(sd == null) {
            List<String> start_end = ts.getSeries(ed);
            sd = start_end.get(0);
        } else if(ed == null) {
            List<String> start_end = ts.getSeries(ed);
            ed = start_end.get(1);
        }
        s_ix = ts.setDay(sd, 0, 1);
        if(s_ix - ts.start() < 2 * adxd) {
            s_ix = 2 * adxd + ts.start();
            sd = ts.get(s_ix).date;
            s_ix = ts.setDay(sd, 0, 1);
        }
        e_ix = ts.find(ed, 0);
        for(int ixx = s_ix; ixx <= e_ix; ixx++) {
            ts.nextDay(1);
            System.err.printf("%s: ADX = %8.2f\n", ts.get(ixx).date,
                              adx.adx(ixx));
        }
    }
}
