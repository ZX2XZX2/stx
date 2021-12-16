package gui;

import core.StxCal;
import core.StxRec;
import core.StxTS;
import jl.StxOBV;
import indicators.StxUDV;
import indicators.StxxSetups;
import jl.StxJL;
import jl.StxxJL;

import java.awt.Color;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.FontMetrics;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.geom.Line2D;
import java.awt.geom.Rectangle2D;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;

import javax.swing.JPanel;

public class Chart extends JPanel {
    private static final long serialVersionUID = 1L;
    StxTS<StxRec> ts;
    int start, end, start0;
    FontMetrics fm;
    TreeMap<String, Integer> trend= null;
    StxxJL jl1, jl2, jl3;
    String stk_name, ed;
    private boolean invisible = false;
    private StxxSetups sss;

    public Chart( String stk_name, String sd, String ed) {
        super( null);
        this.stk_name = stk_name;
        System.err.printf("Creating new chart for %s between %s and %s\n",
                          stk_name, sd, ed);
        ts= StxTS.loadEod( stk_name, null, null);
        // System.err.printf("%s, found %d records\n", stk_name, ts.size());
        start = ts.find( sd, 1); start0= start;
        end= ts.find( ed, -1);
        this.ed = ed;
        if(sss == null || sss.getStk() != stk_name)
            sss = new StxxSetups(stk_name);
        setName(stk_name);
        repaint();
    }

    public Chart( String stk_name, String sd, String ed,
                  boolean adjust, String eod_tbl, String split_tbl,
                  TreeMap<String, Integer> trend) {
        super( null);
        this.stk_name= stk_name;
        ts= StxTS.loadEod( stk_name, null, null, eod_tbl, split_tbl);        
        start= ts.find( sd, 1); start0= start;
        end= ts.find( ed, -1);
        ts.setDay( ed, -1, 1);
        start= end- 60; if( start< 0) start= 0;
        this.trend= trend;
        repaint();
    }

    public Chart( String stk_name, String sd, String ed,
                  boolean adjust, String eod_tbl, String split_tbl,
                  StxxJL jl1, StxxJL jl2, StxxJL jl3, boolean invisible) {
        super(null);
        this.stk_name= stk_name;
        ts= StxTS.loadEod( stk_name, null, null, eod_tbl, split_tbl);      
        start= ts.find( sd, 1); start0= start;
        end= ts.find( ed, -1);
        ts.setDay( ed, -1, 1);
        start= end- 60; if( start< 0) start= 0;
        this.jl1 = jl1;
        this.jl2 = jl2;
        this.jl3 = jl3;
        this.invisible = invisible;
        if(sss == null || sss.getStk() != stk_name)
            sss = new StxxSetups(stk_name);
        repaint();
    }

    public void paint( Graphics g) {
        Graphics2D g2= ( Graphics2D) g;
        Dimension d= getSize();
        int days= end- start;
        double day_width= ( d.width- 155.0)/ ( double) days;
        double bar_width= 0.5* day_width;
        HashMap<Integer, String> labels= null;
        if( days<= 60)
            labels= setWeeklyLabels();
        else if( days<= 756)
            labels= setMonthlyLabels();
        else
            labels= setYearlyLabels();
        setBackground( Color.black);
        g.setColor( getBackground());
        g.fillRect( 0, 0, d.width, d.height);
        double d_height = 95.0 * d.height / 85.0;
        float max_vol= 0, min_vol= 0, vol_rg= 0;
        float min_price= 1000000, max_price= 0, price_rg= 0;
        int ix;
        for( ix= start; ix<= end; ix++) {
            if( max_vol< ts.get( ix).v)
                max_vol= ts.get( ix).v;
            if( max_price< ts.get( ix).h)
                max_price= ts.get( ix).h;
            if( min_price> ts.get( ix).l)
                min_price= ts.get( ix).l;
        }
        vol_rg= max_vol- min_vol;
        price_rg= max_price- min_price;

        double xx= 50, yy= 0.85* d_height;
        double bar_height= 0.2* d_height, hh= 0;
        double yyp= 0.6* d_height, price_height= 0.5* d_height;
        double hho= 0, hhh= 0, hhl= 0, hhc= 0, eps= 4* bar_width/ 9;
        double last_day_x = xx + day_width * (days + 0.25);

        g2.setPaint( Color.darkGray);
        fm = g2.getFontMetrics();

        g2.draw( new Rectangle2D.Double( xx- 5, 0.65* d_height,
                                         d.width- 150+ day_width,
                                         0.2* d_height));
        g2.setPaint( Color.lightGray);
        // draw 3 strings and a line
        g2.drawString( String.format( "%,.0fK", max_vol),
                       ( float) ( d.width- 95+ day_width),
                       ( float) ( 0.65* d_height+ 0.25* fm.getHeight()));
        g2.drawString( String.format( "%,.0fK", max_vol/ 2),
                       ( float) ( d.width- 95+ day_width),
                       ( float) ( 0.75* d_height+ 0.25* fm.getHeight()));
        g2.drawString( "0", ( float) ( d.width- 95+ day_width),
                       ( float) ( 0.85* d_height+ 0.25* fm.getHeight()));
        g2.setPaint( Color.darkGray);
        g2.draw( new Line2D.Double( xx- 5, 0.75* d_height,
                                    d.width- 100+ day_width, 0.75* d_height));

        g2.draw( new Rectangle2D.Double( xx- 5, 0.1* d_height,
                                         d.width- 150+ day_width,
                                         0.5* d_height));
        double step= 0;
        for( ix= 0; ix<= 10; ix++) {
            step= 0.1* d_height+ 0.05* ix* d_height;
            g2.setPaint( Color.lightGray);
            g2.drawString( String.format( "%.2f",
                                          ( max_price- ix* price_rg/ 10)),
                           ( float) ( d.width- 95+ day_width),
                           ( float) ( step+ 0.25* fm.getHeight()));
            g2.setPaint( Color.darkGray);
            g2.draw( new Line2D.Double( xx, step, d.width- 100+ day_width,
                                        step));
        }
        List<Float> avg_volumes = avgVolumes(20);
        for( ix= start; ix<= end; ix++) {
            if( labels.get( ix)!= null) {
                g2.setPaint( Color.darkGray);
                g2.draw( new Line2D.Double( xx+ eps, 0.1* d_height,
                                            xx+ eps, 0.6* d_height));
                g2.draw( new Line2D.Double( xx+ eps, 0.65* d_height,
                                            xx+ eps, yy));
                g2.setPaint( Color.lightGray);
                g2.drawString( labels.get( ix),
                               ( float)( xx- fm.charWidth( '1')*
                                         ( labels.get( ix).length()/ 2- 1)),
                               ( float)( yy+ fm.getHeight()));
            }
            StxRec r= ts.get( ix);
            hho= yyp- price_height* ( r.o- min_price)/ price_rg;
            hhh= yyp- price_height* ( r.h- min_price)/ price_rg;
            hhl= yyp- price_height* ( r.l- min_price)/ price_rg;
            hhc= yyp- price_height* ( r.c- min_price)/ price_rg;
            if( hho< hhc) {
                g2.setPaint( Color.red);
                g2.fill( new Rectangle2D.Double( xx, hho, bar_width, hhc- hho));
                g2.draw( new Line2D.Double( xx, hhc, xx+ bar_width, hhc));
            } else {
                g2.setPaint( Color.green);
                g2.fill( new Rectangle2D.Double( xx, hhc, bar_width, hho- hhc));
                g2.draw( new Line2D.Double( xx, hhc, xx+ bar_width, hhc));
            }
            g2.draw( new Line2D.Double( xx+ eps, hhl, xx+ eps, hhh));
            hh= yy- bar_height* ( r.v- min_vol)/vol_rg;
            g2.draw( new Line2D.Double( xx+ eps, hh, xx+ eps, yy));
            if(ix > start) {
                g2.setPaint(Color.lightGray);
                double av1 = yy - bar_height *
                    (avg_volumes.get(ix - start - 1) - min_vol) / vol_rg;
                double av2 = yy - bar_height *
                    (avg_volumes.get(ix - start) - min_vol) / vol_rg;
                // System.err.printf("Drawing line: %.2f, %.2f, %.2f, %.2f\n",
                //                xx + eps - day_width, av1, xx + eps, av2);
                g2.draw(new Line2D.Double(xx + eps - day_width, av1,
                                          xx + eps, av2));
            }
            xx+= day_width;
        }
        g2.setFont( new Font("Monospaced", Font.PLAIN, 14));
        
        int avg_vol_sz = avg_volumes.size();
        float last_avg_volume = (avg_vol_sz > 0)?
            avg_volumes.get(avg_vol_sz - 1): 0;
        if(jl1 != null) {
//          System.err.printf("Before getChannel(), %s, %s\n", stk_name, ed);
            List<Double> pts = getChannel(jl1, last_day_x, day_width, yyp,
                                          price_height, min_price, price_rg);
//          System.err.printf("After getChannel(), %s, %s\n", stk_name, ed);
            if(pts != null) {
                g2.setPaint( Color.yellow);
                g2.draw(new Line2D.Double(pts.get(0), pts.get(1),
                                          pts.get(2), pts.get(3)));
                g2.draw(new Line2D.Double(pts.get(4), pts.get(5),
                                          pts.get(6), pts.get(7)));
            }
//          System.err.printf("Before getOBV(), %s, %s\n", stk_name, ed);
            String obv_str = getOBV(jl1, last_avg_volume);
//          System.err.printf("After getOBV(), %s, %s\n", stk_name, ed);
            g2.drawString(obv_str, d.width / 2 - 350, 15);
            g2.setPaint( Color.darkGray);
        }
        if(jl2 != null) {
            List<Double> pts = getChannel(jl2, last_day_x, day_width, yyp,
                                          price_height, min_price, price_rg);
            if(pts != null) {
                g2.setPaint( Color.cyan);
                g2.draw(new Line2D.Double(pts.get(0), pts.get(1),
                                          pts.get(2), pts.get(3)));
                g2.draw(new Line2D.Double(pts.get(4), pts.get(5),
                                          pts.get(6), pts.get(7)));
            }
            String obv_str = getOBV(jl2, last_avg_volume);
            g2.drawString(obv_str, d.width / 2 - 350, 35);
            g2.setPaint( Color.darkGray);
        }
        if(jl3 != null) {
            List<Double> pts = getChannel(jl3, last_day_x, day_width, yyp,
                                          price_height, min_price, price_rg);
            if(pts != null) {
                g2.setPaint( Color.white);
                g2.draw(new Line2D.Double(pts.get(0), pts.get(1),
                                          pts.get(2), pts.get(3)));
                g2.draw(new Line2D.Double(pts.get(4), pts.get(5),
                                          pts.get(6), pts.get(7)));
            }
            String obv_str = getOBV(jl3, last_avg_volume);
            g2.drawString(obv_str, d.width / 2 - 350, 55);
            g2.setPaint( Color.darkGray);
        }
        String cndls = sss.getSetups(ts.get(end).date);
        String[] candles = cndls.split("\n");
        if(candles.length == 3) {
            g2.setPaint(Color.white);
            for(int x = 0; x < 3; x++)
                g2.drawString(candles[x], d.width / 2 + 150, 15 + 20 * x);
        }
        g2.setPaint(Color.lightGray);
        if(!invisible)
            g2.drawString(stk_name.toUpperCase(), 50, 15);         
    }

    String getOBV(StxxJL jl, float last_avg_volume) {
        StxOBV obv = new StxOBV(ts, jl);
        List<Integer> pivots = jl.pivots(4, true);
        StringBuffer udv_sb = new StringBuffer("");
//      System.err.printf("%.1f: found %d pivots\n", jl.getFactor(), 
//                        pivots.size());
        if(pivots.size() >= 4) {
            int ixx = 0;
            int udv_end = ts.currentPosition();
            for(int piv: pivots) {
                if(ixx == 0) {
                    ixx++;
                    continue;
                }
                int abs_piv = Math.abs(piv);
                StxJL rec = jl.data(abs_piv);
                float res = obv.obv(piv, udv_end);
                udv_sb.append(" P").append(ixx).append(": ");
                udv_sb.append(String.format("%7.2f [%4.1f]", 
                                            (piv < 0)? rec.c2: rec.c, res));
                ixx++;
            }
        }
        return udv_sb.toString();
    }

    String getUDV(StxxJL jl, StxUDV udv, float last_avg_volume) {
        List<Integer> pivots = jl.pivots(4, true);
        StringBuffer udv_sb = new StringBuffer("");
        // System.err.printf("GetUDV, factor = %.2f\n", jl.getFactor());
        // System.err.printf("Got the following pivots:\n");
        if(pivots.size() >= 5) {
            StxJL piv_0 = jl.data(pivots.get(0));
            // System.err.printf("P0.date = %s\n", piv_0.date);
            int ixx = 0;
            int udv_end = ts.currentPosition();
            for(int piv: pivots) {
                if(ixx == 0) {
                    ixx++;
                    continue;
                }
                StxJL rec = jl.data(piv);
                // System.err.printf("P%d date: %s\n", ixx, rec.date);
                int udv_start = ts.find(rec.date, 0);
                if (udv_start < udv_end)
                    udv_start++;
                List<Float> res = udv.udv(udv_start, udv_end);

                udv_sb.append(" P").append(ixx).append(": ");
                udv_sb.append(String.format("%7.2f [%4.1f]", rec.c,
                                            -res.get(2) / last_avg_volume));
                ixx++;
                if(rec.p2) {
                    udv_sb.append(" P").append(ixx).append(": ");
                    udv_sb.append(String.format("%7.2f [%4.1f]", rec.c2,
                                                res.get(2) / last_avg_volume));
                    ixx++;
                }
            }
        }
        return udv_sb.toString();
    }

    String getUDVOld(StxxJL jl, StxUDV udv, float last_avg_volume) {
        List<Integer> pivots = jl.pivots(4, true);
        StringBuffer udv_sb = new StringBuffer("");
        // System.err.printf("GetUDV, factor = %.2f\n", jl.getFactor());
        // System.err.printf("Got the following pivots:\n");
        if(pivots.size() >= 5) {
            StxJL piv_0 = jl.data(pivots.get(0));
            // System.err.printf("P0.date = %s\n", piv_0.date);
            int ixx = 0, start = ts.find(piv_0.date, 0);
            for(int piv: pivots) {
                if(ixx == 0) {
                    ixx++;
                    continue;
                }
                StxJL rec = jl.data(piv);
                // System.err.printf("P%d date: %s\n", ixx, rec.date);
                int udv_end = ts.find(rec.date, 0);
                List<Float> res = udv.udv(start, udv_end);
                udv_sb.append(" P").append(ixx).append(": ");
                udv_sb.append(String.format("%7.2f [%4.1f]", rec.c,
                                            res.get(2) / last_avg_volume));
                ixx++;
                if(rec.p2) {
                    udv_sb.append(" P").append(ixx).append(": ");
                    udv_sb.append(String.format("%7.2f [%4.1f]", rec.c2,
                                                res.get(2) / last_avg_volume));
                    ixx++;
                }
            }
        }
        return udv_sb.toString();
    }

    List<Double> getChannel(StxxJL jl1, double last_day_x, double day_width,
                            double yyp, double price_height,
                            float min_price, float price_rg) {
        List<Double> x_lst = new ArrayList<Double>();
        List<Double> y_lst = new ArrayList<Double>();
        List<Integer> pivots = jl1.pivots(4, false);
        for(int piv: pivots) {
            int abs_piv = Math.abs(piv);
            StxJL rec = jl1.data(abs_piv);
            if (piv > 0) {
                x_lst.add(last_day_x - day_width * (jl1.size() - abs_piv - 1));
                y_lst.add(yyp - price_height * (rec.c - min_price) / price_rg);
            } else {
                x_lst.add(last_day_x - day_width * (jl1.size() - abs_piv - 1));
                y_lst.add(yyp - price_height * (rec.c2 - min_price) / 
                          price_rg);
            }
        }
        if(x_lst.size() < 4)
            return null;
        List<Double> res = new ArrayList<Double>();
        double x1 = x_lst.get(0), y1 = y_lst.get(0), a, b;
        double x2 = x_lst.get(2), y2 = y_lst.get(2);
        a = (y1 - y2) / (x1 - x2);
        b = (x1 * y2 - x2 * y1) / (x1 - x2);
        double last_day_y = a * last_day_x + b;
        res.add(x1);
        res.add(y1);
        res.add(last_day_x);
        res.add(last_day_y);

        x1 = x_lst.get(1);
        y1 = y_lst.get(1);
        x2 = x_lst.get(3);
        y2 = y_lst.get(3);
        a = (y1 - y2) / (x1 - x2);
        b = (x1 * y2 - x2 * y1) / (x1 - x2);
        last_day_y = a * last_day_x + b;
        res.add(x1);
        res.add(y1);
        res.add(last_day_x);
        res.add(last_day_y);

        return res;
    }

    public List<Float> avgVolumes(int w) {
        List<Float> res = new ArrayList<Float>();
        for(int ix = start; ix <= end; ++ix) {
            float avg_vol = 0;
            int s = ix - w + 1;
            if(s < 0)
                s = 0;
            int ww =  ix - s + 1;
            for(int ixx = s; ixx <= ix; ++ixx)
                avg_vol += ts.get(ixx).v;
            res.add(avg_vol / ww);
        }
        return res;
    }

    
    HashMap<Integer, String> setWeeklyLabels() {
        HashMap<Integer, String> lbls = new HashMap<Integer, String>();
        for(int ix = start; ix <= end; ix++)
            if(StxCal.dow(ts.get( ix).date) == 1)
                lbls.put(ix, invisible? "Week": ts.get( ix).date.substring(5));
        return lbls;
    }

    HashMap<Integer, String> setMonthlyLabels() {
        HashMap<Integer, String> lbls= new HashMap<Integer, String>();
        HashMap<Integer, String> months= new HashMap<Integer, String>();
        months.put(1, "Jan");
        months.put(2, "Feb");
        months.put(3, "Mar");
        months.put(4, "Apr");
        months.put(5, "May");
        months.put(6, "Jun");
        months.put(7, "Jul");
        months.put(8, "Aug");
        months.put(9, "Sep");
        months.put(10, "Oct");
        months.put(11, "Nov");
        months.put(12, "Dec");
        int mm = 0;
        for(int ix = start; ix <= end; ix++) {
            mm = StxCal.month(ts.get(ix).date);
            if((ix > 0) && (StxCal.month(ts.get(ix - 1).date) != mm)) {
                if( mm== 1)
                    lbls.put(ix, (invisible? "Month":
                                  new Integer(StxCal.year(ts.get(ix).date)).
                                  toString()));
                else
                    lbls.put(ix, invisible? "Month": months.get(mm));
            }
        }
        return lbls;
    }

    HashMap<Integer, String> setYearlyLabels() {
        HashMap<Integer, String> lbls = new HashMap<Integer, String>();
        for(int ix = start + 1; ix <= end; ix++) {
            int mm = StxCal.month(ts.get(ix).date);
            if((mm == 1) && (StxCal.month(ts.get(ix - 1).date) != mm))
                lbls.put(ix, (invisible? "Year":
                              new Integer(StxCal.year(ts.get(ix).date)).
                              toString()));
        }
        return lbls;
    }

    
    public void setScale( String scale) {
        if( scale.compareTo( "1M")== 0)
            start= end- 22;
        else if( scale.compareTo( "3M")== 0)
            start= end- 60;
        else if( scale.compareTo( "6M")== 0)
            start= end- 126;
        else if( scale.compareTo( "1Y")== 0)
            start= end- 252;
        else if( scale.compareTo( "JL")== 0)
            start= end- 400;
        else if( scale.compareTo( "2Y")== 0)
            start= end- 504;
        else if( scale.compareTo( "3Y")== 0)
            start= end- 756;
        else if( scale.compareTo( "5Y")== 0)
            start= end- 1260;
        else
            start= 0;
        if( start< 0) start= 0;
        repaint();
    }

    StxRec getSR( String d) {
        StxRec res= null;
        try {
            res= ts.get( ts.find( d, 0));
        } catch( Exception e) {
            res= new StxRec( "1900-01-01", 0);
        }
        return res;
    }

    String rewind() {
        StxRec res = ts.get(90);
        ts.setDay(res.date, 0, 1);
        return res.date;
    }
    
    //StxData<StxRec> data(){ return data; }

    //     public static void main( String args[]) {
    //         JFrame f= new JFrame( "Chart");
    //         f.addWindowListener( new WindowAdapter() {
    //             public void windowClosing( WindowEvent e) {
    //                 System.exit(0);
    //             }
    //         });
    //         String name= "dji", sd= "1998-01-02", ed= "1998-04-01";
    //         int len= args.length;
    //         for( int ix= 0; ix< len; ix++) {
    //             if(( args[ ix].compareTo( "-name")== 0)&& ( ++ix< len))
    //                 name= args[ ix];
    //             else if(( args[ ix].compareTo( "-start")== 0)&& ( ++ix< len))
    //                 sd= args[ ix];
    //             if(( args[ ix].compareTo( "-end")== 0)&& ( ++ix< len))
    //                 ed= args[ ix];
    //         }
    //         Chart chrt= new Chart( name, sd, ed);
    //         chrt.setBackground( Color.black);
    //         f.getContentPane().add( "Center", chrt);
    //         f.setSize( new Dimension( 950, 500));
    //         //        f.pack();
    //         f.setVisible(true);
    //     }
}
