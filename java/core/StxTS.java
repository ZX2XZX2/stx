package core;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class StxTS<T extends StxRecord> {
    public static String dbDataDir=
        "/Program Files/MySQL/MySQL Server 5.7/data/goldendawn/";
    //"/ProgramData/MySQL/MySQL Server 5.6/data/goldendawn/";
    private String stk, sd, ed, d_1, eod_tbl, split_tbl;
    private TreeMap<Integer, Integer> gaps;
    private TreeMap<String, Integer> adj_splits;
    private List<T> data;
    private TreeMap<String, Float> splits; 
    //    private TreeMap<String, Float> divis;
    private int start, end, pos, l;//, gran= 0;
    private static final int MAX_GAP= 22; // for gaps > 22 start new ts
    private static StxCal cal= null;

    public StxTS( String stk, String sd, String ed) {
        init(stk, sd, ed, "eods", "dividends");
    }

    public StxTS( String stk, String sd, String ed, String eod_tbl, 
                  String split_tbl ) {
        init(stk, sd, ed, eod_tbl, split_tbl);
    }

    private void init(String stk, String sd, String ed, String eod_tbl, 
                      String split_tbl) {
        this.stk= stk; this.sd= sd; this.ed= ed;
        this.eod_tbl = eod_tbl; this.split_tbl = split_tbl;
        if( cal== null) {
            cal= new StxCal( new GregorianCalendar().get( Calendar.YEAR)+ 2);
            System.err.println( "Created new StxCal");
        }
        gaps= new TreeMap<Integer, Integer>(); 
        adj_splits= new TreeMap<String, Integer>();
        data= new ArrayList<T>();
        start= 0; end= 0; pos= 0; l= 0;
        loadSplitsDivis();
    }

    public void loadSplitsDivis() {
        splits= new TreeMap<String, Float>();
        //divis= new TreeMap<String, Float>();
        StringBuilder q1= new StringBuilder( "SELECT * FROM ");
        q1.append(this.split_tbl).append(" WHERE ");
        //StringBuilder q2=new StringBuilder("SELECT t FROM Dividend t WHERE ");
        q1.append( "stk='").append( stk).append( "' ");
        // q2.append( "t.id.stk='").append( stk).append( "' ");
        if( sd!= null) {
            q1.append( "AND dt>='").append( sd).append( "' "); 
            //   q2.append( "AND t.id.dt>='").append( sd).append( "' "); 
        }
        if( ed!= null) {
            q1.append( "AND dt<='").append( ed).append( "'"); 
            //   q2.append( "AND t.id.dt<='").append( ed).append( "'"); 
        }
        try {
            StxDB sdb = new StxDB(System.getenv("POSTGRES_DB"));
            ResultSet rset = sdb.get(q1.toString());
            while(rset.next())
                splits.put( StxCal.nextBusDay(rset.getString(2)),
                            rset.getFloat(3));
        //List<Dividend> d_lst= em.createQuery( q2.toString()).getResultList();
        //        for( Dividend divi: d_lst)
        //            divis.put( divi.getId().getDt(), divi.getDiv());
        } catch( Exception ex) {
            System.err.println( "Failed to load splits for stock "+ stk+ ":");
            ex.printStackTrace( System.err);
        }
    }
    
    public void add( T t) { if( l++== 0) d_1= t.date; data.add( t); ++end;}
    public int find( String d, int c) {
        if( d_1== null) d_1= data.get( 0).date;
        int n= StxCal.numBusDays( d_1, d);
        if( n< 0) { if( c> 0) return 0; }
        else if( n>= l) { if( c< 0) return l- 1; }
        else {
            int dn= StxCal.numDate( d);
            int nn= StxCal.numDate( data.get( n).date);
            if( dn== nn) return n;
            if( nn> dn){ if( c> 0) return n; if( c< 0) return n- 1;}
            if( nn< dn){ if( c< 0) return n; if( c> 0) return n+ 1;}
        }
        return -1;
    }
    public int setDay( String d, int c, int adj_dir) {
        int new_pos= find( d, c);
        if( new_pos== -1) return new_pos;
        int s=( pos< new_pos)? pos: new_pos, e=( pos> new_pos)? pos: new_pos;
        List<Integer> gap_s= checkForGaps( s, e, pos<= new_pos);
        if( gap_s!= null)
            clearAdjSplits( gap_s);
        if( gap_s!= null)
            { s= gap_s.get( 0); if( new_pos< s) new_pos= s; e= new_pos;}
        String ssd= data.get( s).date, eed= data.get( e).date;
        ssd= splits.ceilingKey( ssd); eed= splits.floorKey( eed);
        if(( ssd== null)|| ( eed== null)|| ( StxCal.cmp( ssd, eed)< 0)) {
            pos= new_pos; // nothing to do, no splits detected
            return pos;
        } // if splits detected:
        for( String crs= ssd; ( crs!= null&& StxCal.cmp( crs, eed)> -1); 
             crs= splits.higherKey( crs)) {
            Integer split_dir= adj_splits.get( crs);
            if( split_dir!= null) {
                if( split_dir== 1) backwardSplitAdjust( crs);
                else forwardSplitAdjust( crs);
            } else {
                if( adj_dir== 1) backwardSplitAdjust( crs);
                else forwardSplitAdjust( crs);
            }
        }
        pos= new_pos;
        return pos;
    }
    public int nextDay( int adj_dir) {
        if( pos== ( l- 1)) return -1;
        checkForGaps( pos, pos+ 1, true);
        String dt= data.get( ++pos).date;
        Float split_ratio= splits.get( dt);
        if( split_ratio== null) return pos;
        if( adj_dir== 1) backwardSplitAdjust( dt);
        else forwardSplitAdjust( dt);
        return pos;
    }
    private List<Integer> checkForGaps( int six, int eix, boolean move_fwd) {
        List<Integer> res= null;
        Map.Entry<Integer, Integer> g1= gaps.floorEntry( six);
        Map.Entry<Integer, Integer> g2= gaps.floorEntry( eix);
        if(( g1== null)&& ( g2!= null)) {
            res= new ArrayList<Integer>();
            if( move_fwd== true) { 
                start= g2.getKey()+ g2.getValue();
                g1= gaps.ceilingEntry( start);
                end= ( g1== null)? l: ( g1.getKey()+ g1.getValue());
            } else {
                start= 0;
                g1= gaps.ceilingEntry( six);
                end= g1.getKey()+ g1.getValue();
            }
            res.add( start); res.add( end);
        }
        else if(( g1!= null)&& ( g2!= null)&& ( g1.getKey()< g2.getKey())) {
            res= new ArrayList<Integer>();
            if( move_fwd== true) { 
                start= g2.getKey()+ g2.getValue();
                g1= gaps.ceilingEntry( start);
                end= ( g1== null)? l: ( g1.getKey()+ g1.getValue());
            } else {
                start= g1.getKey()+ g1.getValue();
                g1= gaps.ceilingEntry( six);
                end= g1.getKey()+ g1.getValue();
            }
            res.add( start); res.add( end);
        }
        if(( res!= null)&& ( pos< start)) pos= start- 1;
        return res;
    }
    public void addGap( int s_gap, int l_gap) { 
        if( l_gap> MAX_GAP) gaps.put( s_gap, l_gap);
    }
    public String printGaps() {
        StringBuilder sb= new StringBuilder();
        for( Map.Entry<Integer, Integer> g: gaps.entrySet())
            sb.append( stk).append( ',').append( data.get( g.getKey()).date)
                .append( ',').append( data.get( g.getKey()+g.getValue()).date)
                .append( ',').append( g.getValue()).append( '\n');
        return sb.toString();
    }

    private void clearAdjSplits( List<Integer> gap_s) {
        for( Map.Entry<String, Integer> ee:
                 new TreeMap<String, Integer>( adj_splits).entrySet()) {
            String ad= ee.getKey(), sd= data.get( start).date;
            String ed= data.get( end- 1).date;
            if(( StxCal.cmp( sd, ad)>= 0)&& ( StxCal.cmp( ad, ed)>= 0)) {
                if( ee.getValue()== 1) backwardSplitAdjust( ee.getKey());
                else forwardSplitAdjust( ee.getKey());
            }
        }
    }
    private void backwardSplitAdjust( String dt) {
        float split_ratio= splits.get( dt);
        boolean undo_adjustment= ( adj_splits.get( dt)!= null);
        if( undo_adjustment) split_ratio= 1/ split_ratio;
        int n= find( dt, 0); if( n== -1) return;
        for( int ix= n- 1; ix>= start; ix--) data.get( ix).split(split_ratio);
        if( undo_adjustment) adj_splits.remove( dt);
        else adj_splits.put( dt, 1);
    }
    private void forwardSplitAdjust( String dt) {
        float split_ratio= splits.get( dt);
        boolean undo_adjustment= ( adj_splits.get( dt)!= null);
        if( !undo_adjustment) split_ratio= 1/ split_ratio;
        int n= find( dt, 0); if( n== -1) return;
        for( int ix= n; ix< end; ix++) data.get( ix).split( split_ratio);
        if( undo_adjustment) adj_splits.remove( dt);
        else adj_splits.put( dt, -1);
    }
    public List<String> getEndDates() {
        String[] end_dates= new String[ gaps.size()]; int ix= 0;
        for( Integer gap: gaps.keySet()) 
            end_dates[ ix++]= data.get( gap).date;
        return Arrays.asList( end_dates);
    }
    public List<String> getLastSeries() {
        Map.Entry<Integer, Integer> last_gap = gaps.lastEntry();
        ArrayList<String> start_end = new ArrayList<String>();
        if(last_gap == null) {
            start_end.add(data.get(start).date);
            start_end.add(data.get(l - 1).date);
        } else {
            start_end.add(data.get(last_gap.getKey()).date);
            start_end.add(data.get(last_gap.getValue()).date);
        }
        return start_end;
    }
    public List<String> getSeries(String date) {
        int ix = find(date, 0);
        if(ix == -1)
            return null;
        Map.Entry<Integer, Integer> last_gap = gaps.floorEntry(ix);
        ArrayList<String> start_end = new ArrayList<String>();
        if(last_gap == null) {
            start_end.add(data.get(start).date);
            start_end.add(data.get(l - 1).date);
        } else {
            start_end.add(data.get(last_gap.getKey()).date);
            start_end.add(data.get(last_gap.getValue()).date);
        }
        return start_end;
    }

    public T last() { return ( l<= 0)? null: data.get( l- 1); }
    public String firstDate() { return data.get( start).date;}
    //private void dividend( int n) {
    //    T t= data.get( n); t.ef= false;
    //    for( int ix= n; ix>= 0; ix--) data.get( ix).dividend( t.ev);
    //}
    public T get( int ix) { return ( ix>= l)? null: data.get( ix); }
    public T rel( int o) { return ( pos- o< start)? null: data.get( pos- o);} 
    public int w( int ix) { return ( pos- ix< start)? ( pos- start+ 1): ix;}
    public boolean first( int ix) { return ( pos- ix)== start;}
    public int start() { return start; }
    public String stk() { return stk;}
    public int size() { return data.size();}
    public String toString() {
        StringBuilder sb= new StringBuilder();
        for( int ix= start; ix< end; ix++) sb.append(data.get(ix).toString());
        return sb.toString();
    }
    public String currentDate() { return data.get(pos).date; }
    public List<T> data() { return data; }
    public int currentPosition() { return pos; }
    //public TreeMap<String, Float> getDividends() { return dividends;}
    //public void setDividends( TreeMap<String, Float> dividends) {
    //    this.dividends= new TreeMap<String, Float>( dividends);
    //}
    public TreeMap<String, Float> getSplits() { return splits;}
    public Float getSplit( String dt) { return splits.get( dt);}
    public void setSplits( TreeMap<String, Float> splits) {
        this.splits= new TreeMap<String, Float>( splits);
    }

    static public StxTS<StxRec> loadEod( String stk) {
        return loadEod( stk, null, null, "eods", "dividends");
    }
    static public StxTS<StxRec> loadEod( String stk, String s_date) {
        return loadEod( stk, s_date, null, "eods", "dividends");
    }
    static public StxTS<StxRec> loadEod( String stk, String sd, String ed) {
        return loadEod( stk, sd, ed, "eods", "dividends");
    }
    static public StxTS<StxRec> loadEod( String stk, String sd, String ed, 
                                         String eod_tbl, String split_tbl) {
        StxTS<StxRec> ts= new StxTS<StxRec>( stk, sd, ed, eod_tbl, split_tbl);
        try {
            StringBuilder q= new StringBuilder( "SELECT * FROM ");
            q.append(eod_tbl).append(" WHERE ");
            q.append( "stk='").append( stk).append( "'");
            if( sd!= null) 
                q.append( " AND dt>='"+ sd+ "'");
            else
                q.append( " AND dt>='1901-01-02'");
            if( ed!= null) q.append( " AND dt<='"+ ed+ "'");
            q.append(" ORDER BY dt");
            StxDB sdb = new StxDB(System.getenv("POSTGRES_DB"));
            ResultSet rset = sdb.get(q.toString());
            int s_gap= 0, l_gap= 0, ix= 0;
            while(rset.next()) {
                StxRec sr = new StxRec(rset.getString(2), rset.getInt(3),
                                       rset.getInt(4),  rset.getInt(5),
                                       rset.getInt(6),  rset.getInt(7));
                if( ts.last()!= null) {
                    String d= StxCal.nextBusDay( ts.last().date);
                    int dt_cmp= StxCal.cmp( sr.date, d);
                    if( dt_cmp== 0)
                        ts.add( sr);
                    else if( dt_cmp== -1) {
                        s_gap= ix; l_gap= 0;
                        while( sr.date.equals( d)== false) {
                            ts.add( new StxRec( d, ts.last().c));
                            d= StxCal.nextBusDay( d);
                            ++ix; ++l_gap;
                        }
                        ts.addGap( s_gap, l_gap);
                        ts.add( sr); 
                    }
                } else
                    ts.add( sr);
                ++ix;
            }
        } catch( Exception ex) {
            System.err.println( "Failed to load stock "+ stk+ ":");
            ex.printStackTrace( System.err);
        }
        return ts; 
    }
    public static void main( String[] args) {
        Calendar c1= new GregorianCalendar();
        StxTS<StxRec> spy= StxTS.loadEod( "SPY", null, null);
        Calendar c2= new GregorianCalendar();
        System.err.println( "Loading "+ spy.size()+ " SPY records took "+ 
                            ( c2.getTimeInMillis()- c1.getTimeInMillis())+ 
                            " milliseconds");
        StxTS<StxRec> fas= StxTS.loadEod( "FAS", null, null);
        Calendar c3= new GregorianCalendar();
        System.err.println( "Loading "+ fas.size()+ " FAS records took "+ 
                            ( c3.getTimeInMillis()- c2.getTimeInMillis())+ 
                            " milliseconds");
        StxTS<StxRec> vxx= StxTS.loadEod( "VXX", "2012-10-01", "2012-10-10");
        System.err.println( "After loading VXX:");
        System.err.println( vxx.toString());
        vxx.setDay( "2012-10-04", 0, 1);
        System.err.println( "After setting the date to 2012-10-04:");
        System.err.println( vxx.toString());
        vxx.setDay( "2012-10-02", 0, 1);
        System.err.println( "After setting the date to 2012-10-02:");
        System.err.println( vxx.toString());
        vxx.setDay( "2012-10-08", 0, 1);
        System.err.println( "After setting the date to 2012-10-08:");
        System.err.println( vxx.toString());
        vxx.setDay( "2012-10-01", 0, 1);
        System.err.println( "After setting the date to 2012-10-01:");
        System.err.println( vxx.toString());
        int ix;
        while(( ix= vxx.nextDay( 1))!= -1)
            System.err.println( "After "+ vxx.get(ix).date+ " next( 1):\n"+
                                vxx.toString());
        vxx.setDay( "2012-10-01", 0, 1);
        System.err.println( "After setting the date to 2012-10-01( 2):");
        System.err.println( vxx.toString());
        while(( ix= vxx.nextDay( -1))!= -1)
            System.err.println( "After "+ vxx.get(ix).date+ " next( -1):\n"+
                                vxx.toString());
        vxx.setDay( "2012-10-09", 0, 1);
        System.err.println( "After setting the date to 2012-10-09:");
        System.err.println( vxx.toString());
        vxx.setDay( "2012-10-04", 0, 1);
        System.err.println( "After setting the date to 2012-10-04:");
        System.err.println( vxx.toString());
        StxTS<StxRec> twtr= StxTS.loadEod( "TWTR", null, null);
        System.err.println( twtr.printGaps());
        twtr.setDay( "2013-12-06", 0, 1);
        System.err.println( "After setting the date to 2013-12-06:");
        System.err.println( twtr.toString());
        twtr.setDay( "2006-12-20", 0, 1);
        System.err.println( "After setting the date to 2006-12-20:");
        System.err.println( twtr.toString());
        twtr.setDay( "2013-12-06", 0, 1);
        System.err.println( "After setting the date to 2013-12-06( 2):");
        System.err.println( twtr.toString());
        twtr.setDay( "2006-12-13", 0, 1);
        System.err.println( "After setting the date to 2006-12-13:");
        System.err.println( twtr.toString());
        twtr.setDay( "2013-12-06", 0, 1);
        System.err.println( "After setting the date to 2013-12-06( 3):");
        System.err.println( twtr.toString());
        twtr.setDay( "1999-12-02", 0, 1);
        System.err.println( "After setting the date to 1999-12-02:");
        System.err.println( twtr.toString());        
    }
    public static void main2( String[] args) {
        StxTS<StxRec> data= StxTS.loadEod( "EXPE", null, null);
        List<String> end_dates= data.getEndDates();
        for( String end_date: end_dates) {
            System.err.println( end_date+ "\n==========");
            data.setDay( StxCal.prevBusDay( end_date), 0, 1);
            System.err.println( data.toString());
        }
    }
    // public static void main(String[] args) {
    //     BufferedReader br= null;
    //     String fn= StxFile.RootDir+ "upload_list.txt";
    //     int ix= 0; boolean first= true;
    //     TreeMap<String, List<Integer>> calc_inds=
    //         new TreeMap<String, List<Integer>>();
    //     List<Integer> rs_tens= new ArrayList<Integer>();
    //     rs_tens.add( 5); rs_tens.add( 20); rs_tens.add( 60); rs_tens.add( 252);
    //     calc_inds.put( "RS", rs_tens);
    //     List<Integer> udv_tens= new ArrayList<Integer>();
    //     udv_tens.add( 5); udv_tens.add( 10); udv_tens.add( 20);
    //     udv_tens.add( 60); udv_tens.add( 252); calc_inds.put( "UDV", udv_tens);
    //     List<Integer> vlt_tens= new ArrayList<Integer>();
    //     vlt_tens.add( 5); vlt_tens.add( 20); vlt_tens.add( 60);
    //     vlt_tens.add( 252); calc_inds.put( "VLT", vlt_tens);
    //     List<Integer> bb_tens= new ArrayList<Integer>();
    //     bb_tens.add( 5); bb_tens.add( 20); bb_tens.add( 60); bb_tens.add( 252);
    //     calc_inds.put( "BB", bb_tens);

    //     String sd= "2013-01-02"; int num= 0;
    //     //String sd= "1984-10-11"; int num= 0;
    //     SimpleDateFormat sdf= new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss");
    //     PrintWriter pw= null;
    //     String wfn= StxTS.dbDataDir+ "indxxx.txt";
    //     try {
    //         System.err.println( "Start: "+ sdf.format( new GregorianCalendar()
    //                                                    .getTime()));
    //         br= new BufferedReader( new FileReader( fn));
    //         pw= new PrintWriter( new FileWriter( wfn, false), false);
    //         for( String line; ( line= br.readLine())!= null;) {
    //             if( first) { first= false; continue; } num= 0;
    //             StxTS<StxRec> ts= StxTS.loadEod( line, null, null);
    //             num+= StxCalc.calcLiquidity( ts, sd, 45, pw);
    //             if( ++ix% 1000== 0)
    //                 System.err.println
    //                     ( sdf.format( new GregorianCalendar().getTime())+ 
    //                       ": processed "+ ix+ " stocks");
    //             System.err.printf( "%5s(%6d): %s\n", ts.stk(), num, 
    //                                sdf.format( new GregorianCalendar()
    //                                            .getTime()));
    //         }
    //         br.close();
    //         if( pw!= null) { pw.flush(); pw.close();}
    //         System.err.println( sdf.format( new GregorianCalendar().getTime())+ 
    //                             ": processed "+ ix+ " stocks");
    //     } catch( Exception ex) {
    //             ex.printStackTrace( System.err);
    //     }
    // }
}
