package gui;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.ResultSet;
import core.StxCal;
import core.StxDB;

public class StxTrade {
    String stk, und, cp, expiry, in_date, crt_date, trade_date;
    float strike, in_spot, in_range, crt_spot, in_bid, in_ask, crt_bid, 
        crt_ask;
    int num_contracts;
    boolean active, skip;
    
    public StxTrade(String stk, String cp, String expiry, String in_date,
                    Float strike, float in_spot, float in_range,
                    Float capital, boolean skip, String trade_date) {
        this.stk = stk;
        this.und = stk.matches("[A-Z]+\\.[0-9]{6}")?
            stk.substring(0, stk.indexOf('.')): stk;
        this.cp = cp.equals("CALL")? "c": "p";
        this.expiry = expiry;
        this.in_date = in_date;
        this.strike = strike.floatValue();
        this.in_spot = in_spot;
        this.in_range = in_range;
        this.trade_date = trade_date;
        crt_spot = in_spot;
        StringBuilder q1 =
            new StringBuilder("SELECT bid, ask FROM options WHERE und='");
        q1.append(this.und).append("' AND expiry='").append(expiry).
            append("' AND dt='").append(in_date).append("' AND cp='").
            append(this.cp).append("' and strike=").append((int)(100 * strike));
        // System.err.printf("Ctor: q1 = %s\n", q1.toString());
        try {
            StxDB sdb = new StxDB("stx");
            ResultSet rset = sdb.get(q1.toString());
            while(rset.next()) {
                in_bid = (float) (rset.getInt(1) / 100.0);
                in_ask = (float) (rset.getInt(2) / 100.0);
                // System.err.printf("in_bid = %f, in_ask = %f\n", in_bid, in_ask);
            }
        } catch( Exception ex) {
            System.err.println("Failed to bid/ask for " + key() + ":");
            ex.printStackTrace(System.err);
        }
        crt_bid = in_bid;
        crt_ask = in_ask;
	if (in_ask > 0)
	    num_contracts = (int) (0.01 * capital.floatValue() / in_ask);
	else
	    num_contracts = 0;
        // System.err.printf("Ctor: num_contracts = %d\n", num_contracts);
        crt_date = in_date;
        active = true;
        this.skip = skip;
    }

    public String key() {
        return String.format("%s_%s_%s_%.2f", stk, cp, expiry, strike);
    }

    public void update(String date, float spot, String log_fname) {
        if(active) {
            crt_date = date;
            crt_spot = spot;
            StringBuilder q1 =
                new StringBuilder("SELECT bid, ask FROM options WHERE und='");
            q1.append(this.und).append("' AND expiry='").append(expiry).
                append("' AND dt='").append(date).append("' AND cp='").
                append(cp).append("' and strike=").append((int)(100 * strike));
            try {
                StxDB sdb = new StxDB("stx");
                ResultSet rset = sdb.get(q1.toString());
                while(rset.next()) {
                    crt_bid = (float) (rset.getInt(1) / 100.0);
                    crt_ask = (float) (rset.getInt(2) / 100.0);
                }
            } catch( Exception ex) {
                System.err.println("Failed to bid/ask for " + key() + ":");
                ex.printStackTrace(System.err);
            }
            // if((StxCal.numBusDays(crt_date, expiry) == 0) ||
            //    (StxCal.numBusDays(crt_date, expiry) == 1 &&
            //  StxCal.cmp(expiry, "2015-01-17") > 0))
            if(StxCal.numBusDaysExpiry(crt_date, expiry) == 0)
                close(log_fname);
        }
    }

    public void close(String log_fname) {
        active = false;
        try {
            log_entry(log_fname);
        } catch(IOException ioe) {
            System.err.printf("Failed to log trade %s on date %s\n",
                              key(), crt_date);
            ioe.printStackTrace(System.err);
        }
    }

    public boolean is_active() { return active; }
    
    public String ui_entry() {
        int sgn = cp.equals("c")? 1: -1;
        StringBuilder sb = 
            new StringBuilder(String.format("%s %5s ", active? " ": "#", und));
        sb.append(cp.equals("c")? "C ": "P ").
            append(String.format("%6.2f %3d AE: %2d/ %2d ",
				 strike, num_contracts,
                                 StxCal.numBusDays(in_date, crt_date),
                                 StxCal.numBusDaysExpiry(crt_date, expiry))).
            append(String.format("S: %6.2f[%6.2f] ", crt_spot, in_spot)).
            append(String.format("O: %5.2f/%5.2f[%5.2f], PL:%6.0f\n",
                                 crt_bid, crt_ask, in_ask,
                                 100 * num_contracts * (crt_bid - in_ask)));
        return sb.toString();
    }

    public void log_entry(String log_fname)
        throws IOException {
        // System.err.println("log_entry(): log_fname = " + log_fname);
        int sgn = cp.equals("c")? 1: -1;
        StringBuffer sb = new StringBuffer();
        float spot_pnl = (sgn * (crt_spot - in_spot) / in_range - 1) / 2;
        if(spot_pnl < -1) spot_pnl = -1;
        float opt_pnl = 100 * num_contracts * (crt_bid - in_ask);

        sb.append(stk).append(',').append(cp.equals("c")? "CALL": "PUT").
            append(',').append(in_date).append(',').append(in_spot).append(',').
            append(String.format("%.2f", in_range)).append(',').
            append(crt_date).append(",").append(crt_spot).append(",").
            append(String.format("%.2f", spot_pnl)).append(",").
            append(in_date.substring(0, 4)).append(",");
        if(spot_pnl > 0)
            sb.append(String.format("1,%.2f,,", spot_pnl));
        else
            sb.append(String.format("0,,%.2f,", spot_pnl));
        sb.append(strike).append(",").append(expiry).append(",").
            append(in_ask).append(",").append(crt_bid).append(",").
            append(num_contracts).append(",").
            append(String.format("%.2f", opt_pnl)).append(",");
        if(opt_pnl > 0)
            sb.append(String.format("1,%.2f,,", opt_pnl));
        else
            sb.append(String.format("0,,%.2f,", opt_pnl));
        sb.append(skip? "1,": "0,").append(trade_date).append(",");
        PrintWriter pw = new PrintWriter(new FileWriter(log_fname, true));
        pw.println(sb.toString());
        pw.close();     
    }
}
