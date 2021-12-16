package core;

import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

public class StxData<T extends StxRecord> {
    private ArrayList<T> data;
    private String d_1; private int l;
    private TreeMap<String, Float> splits= new TreeMap<String, Float>();
    private TreeMap<String, Float> dividends= new TreeMap<String, Float>();
    public StxData() { l= 0; data= new ArrayList<T>( 128); }
    public StxData( int n) { l= 0; data= new ArrayList<T>( n); }
    public void add( T t) { if( l++== 0) d_1= t.date; data.add( t); }
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
    public void split( int n) {
        T t= data.get( n); t.ef= false; 
        if( t.date.indexOf( "-")>= 0)splits.put( t.date, t.ev); 
        for( int ix= n; ix>= 0; ix--) data.get( ix).split( t.ev);
    }
    private void dividend( int n) {
        T t= data.get( n); t.ef= false;
        for( int ix= n; ix>= 0; ix--) data.get( ix).dividend( t.ev);
    }
    private void adjustAll( int n) {
        for( int ix= 0; ix<= n; ix++) adjust( ix);
    }
    public void adjust( int n) {
        if( n--== 0) return; T t= data.get( n); if( !t.ef) return;
        if( t.en== '*') split( n); else if( t.en== 'D') dividend( n);
    }
    public int range( String d, int c) {
        int e= find( d, c); adjustAll( e); return e;
    }
    public int range( int ix) { adjustAll( ix); return( ix< 0)? -1: ix;}
    public T get( int ix) { return (ix>= l)? null: data.get( ix); }
    public T last() { return ( l<= 0)? null: data.get( l- 1); }
    public int next( int ix) {
        if( ++ix>= l|| ix< 0) return -1; adjust( ix); return ix;
    }     
    public int size() { return data.size();}
    public String toString() {
        StringBuilder sb= new StringBuilder();
        for( T t: data) sb.append( t.toString());
        return sb.toString();
    }
    public ArrayList<T> data() { return data; }
    public void removeRange( int s, int e) {
        if( s< e) {
                l= data.size()- e+ s; d_1= null; 
                data.subList( s, e).clear();
        }
    }
    public void clear() { data.clear(); l= 0; }
    public TreeMap<String, Float> getDividends() { return dividends;}
    public void setDividends( TreeMap<String, Float> dividends) {
        this.dividends= new TreeMap<String, Float>( dividends);
    }
    public TreeMap<String, Float> getSplits() { return splits;}
    public void setSplits( TreeMap<String, Float> splits) {
        this.splits= new TreeMap<String, Float>( splits);
    }
    public ArrayList<String> getSplits( String sd, String ed) { 
        ArrayList<String> split_list= new ArrayList<String>();
        for( Map.Entry<String, Float> splt: splits.entrySet()) {
            String splt_dt= splt.getKey();
            if((StxCal.cmp(sd,splt_dt)>=0)&&(StxCal.cmp(splt_dt,ed)==1))
                split_list.add( splt_dt);
        }       
        return split_list;
    }
    public float getSplit( String d) { return splits.get( d);}
    public boolean hasSplit1( String dt) {
        return splits.get( dt)!= null;
    }
    public boolean hasSplit( String sd, String ed) {
        boolean res= false;
        for( Map.Entry<String, Float> splt: splits.entrySet()) {
            String splt_dt= splt.getKey();
            if((StxCal.cmp(sd, splt_dt)>=0)&&(StxCal.cmp(splt_dt,ed)==1))
                res= true;
        }
        return res;
    }
    public void setSplit( String d, float val) { splits.put( d, val);}
    public void gran( int gran) { }

    static public StxData<StxRec> weekly( StxData<StxRec> data) {
        return weekly( data, "2990-01-31");
    }

    static public StxData<StxRec> weekly( StxData<StxRec> data, String ed) {
        StxData<StxRec> weekly= new StxData<StxRec>();
        int end= data.find( ed, -1);

        try {
            String sow0= StxCal.sow( data.get( 0).date), sow= null;
            StxRec sr= new StxRec( data.get( 0)); sr.date= sow0;
            for( int ix= 0; ix<= end; ix++) {
                sow= StxCal.sow( data.get( ix).date);
                if( sow.equals( sow0)== false) {
                    weekly.add( sr);
                    sr= new StxRec( data.get( ix));
                    sow0= sow;
                } else if( ix> 0) {
                    sr.c= data.get( ix).c;
                    sr.v+= data.get( ix).v;
                    if( sr.h< data.get( ix).h)
                        sr.h= data.get( ix).h;
                    if( sr.l> data.get( ix).l)
                        sr.l= data.get( ix).l;
                }
            }
            weekly.add( sr);
//             for( StxRec srr: weekly.data())
//                 System.err.print( srr.toString());
        } catch( Exception ex) {
            ex.printStackTrace( System.err);
        }
        return weekly;
    }

}
