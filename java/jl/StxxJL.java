package jl;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;

import core.StxCal;
import core.StxData;
import core.StxRec;
import core.StxTS;
import indicators.StxHL;


public class StxxJL {
    private StxData<StxJL> recs;
    private StxTS<StxRec> data;
    public  StxJL lns, ls, pns, ps;
    private float [] lp= new float[ 8];
    private StxHL<StxRec> hl; private String stk;
    private float _f, f, avg_rg, vi; private int c= -1, w;

    public StxxJL( String stk, String sd, String ed, float f, int w, int vw,
                   String eod_tbl, String split_tbl) {
        data= StxTS.loadEod( stk, sd, ed, eod_tbl, split_tbl);
        this.f= f; this.w= w; this.stk= stk;
        recs= new StxData<StxJL>();
    }

    public StxData<StxJL> jl( String as_of_date) {
        recs.clear(); lns= null; ls= null; pns= null; ps= null;
        c = data.find( as_of_date, -1); int start= data.start();
        // c= data.setDay( as_of_date, -1, 1); int start= data.start();
        //System.err.println( "Running JL as of: "+ data.get( c).date);
        init(); for( int ix= start+ w; ix<= c; ++ix) next( ix);
        return recs;
    }

    private void init() {
        int start= data.start();
        lns= null; ls= null; int we= ( w< ( c- start))? w: c- start;
        if( start+ we< 1) return;
        data.setDay( data.get( start+ we- 1).date, 0, 1);
        avg_rg= avgRange( data.data(), start+ we- 1, w);
        hl= new StxHL<StxRec>(); hl.hl( data.data(), w, start+ we- 1);
        lp[ StxJL.UT]= ( lp[ StxJL.NRa]= ( lp[ StxJL.SRa]= ( lp[ StxJL.m_NRa]=
                                                             hl.hi)));
        lp[ StxJL.DT]= ( lp[ StxJL.NRe]= ( lp[ StxJL.SRe]= ( lp[ StxJL.m_NRe]=
                                                             hl.lo)));
        for( int ix= start; ix< start+ we; ix++)
            if( ix== hl.d_hi&& ix== hl.d_lo) recDay( ix, StxJL.NRa, StxJL.NRe);
            else if( ix== hl.d_hi) recDay( ix, StxJL.NRa, StxJL.None);
            else if( ix== hl.d_lo) recDay( ix, StxJL.None, StxJL.NRe);
            else recDay( ix, StxJL.None, StxJL.None);
    }

    public float avgRange(List<StxRec> data, int ix, int w) {
        float res = 0; 
        int s = ix - w + 1; 
        if (s < 0) 
            s = 0;
        int ww = ix - s + 1;
        for(int ixx = s; ixx <= ix; ++ixx)
            res += data.get(ixx).trueRange((ixx == 0)? null: 
                                           data.get(ixx - 1));
        return res / ww;
    }

    public float avgVolume(List<StxRec> data, int ix, int w) {
        float res = 0;
        int s = ix - w + 1; 
        if (s < 0) 
            s = 0;
        int ww = ix - s + 1;
        for(int ixx = s; ixx <= ix; ++ixx)
            res += data.get(ixx).v;
        return res / ww;
    }

    public void next(int ix) {
        _f = f * avg_rg;
        data.nextDay(1);
        Float ratio = data.getSplit(data.rel(0).date);
        if (ratio != null) {
            adjustForSplits(ratio.floatValue());
            adjustRecordsForSplit(ix, ratio.floatValue());
        }
        switch(ls.s()) {
        case StxJL.SRa: 
            sRa(ix, _f); 
            break; 
        case StxJL.NRa: 
            nRa(ix, _f);
            break;
        case StxJL.UT:
            uT(ix, _f);
            break;
        case StxJL.DT:
            dT( ix, _f);
            break;
        case StxJL.NRe:
            nRe( ix, _f);
            break;
        case StxJL.SRe:
            sRe( ix, _f);
            break;
        };
        avg_rg = avgRange(data.data(), ix, w);
        vi = avgVolume(data.data(), ix, w);
    }

    private void adjustForSplits(float ratio) {
        for(int ix = 0; ix < lp.length; ++ix)
            lp[ix] *= ratio;
        //lns.split( ratio);
    }

    private void recDay(int ix, int sh, int sl) {
        StxRec sr = data.get(ix);
        StxJL jlr= new StxJL( sr.date, avg_rg, vi, recs.size());
        float prev_c = (ix == 0)? sr.o: data.get(ix - 1).c;
        jlr.setOBV(sr, prev_c); 
        recs.add(jlr);
        if(( sh!= StxJL.None)&& ( sl!= StxJL.None)) {
            if( sr.hiB4Lo()){ r( jlr, sr, sh); r( jlr, sr, sl);}
            else{ r( jlr, sr, sl); r( jlr, sr, sh);}
        } else if( sh!= StxJL.None) r( jlr, sr, sh); 
        else if( sl!= StxJL.None) r( jlr, sr, sl); 
        else r( jlr, sr, StxJL.None);
        if(( sh!= StxJL.None)|| ( sl!= StxJL.None)) {
            if(( sh!= StxJL.None)&& ( sl!= StxJL.None)) {
                ls= recs.last(); ps= ls;
            } else {
                ps= ls; ls= recs.last();
            }
        }
        jlr.lnx( pns== null? null: pns.xx(), ps== null? null: ps.xx());
        pns= lns; ps= ls;
    }
    private void r( StxJL jlr, StxRec sr, int s) {
        if( lns!= null)
            lns.p(( up( lns.sn())&& dn( s))|| ( dn( lns.sn())&& up( s)),
                  jlr.xx());
        if( up( s)|| dn( s)) { pns= lns; lns= recs.last();} jlr.s( s);
        if( upAll( s)) jlr.c( sr.h); if( dnAll( s)) jlr.c( sr.l);
        if( s!= StxJL.None) lp[ s]= jlr.c();
    }

    public boolean up( int s) { return s== StxJL.UT|| s== StxJL.NRa;}
    public boolean dn( int s) { return s== StxJL.DT|| s== StxJL.NRe;}
    public boolean upAll( int s) {
        return s== StxJL.UT|| s== StxJL.NRa|| s== StxJL.SRa;
    }
    public boolean dnAll( int s) {
        return s== StxJL.DT|| s== StxJL.NRe|| s== StxJL.SRe;
    }

    private void sRa( int ix, float f) {
        StxRec r= data.get( ix); int sh= StxJL.None, sl= StxJL.None;
        if(( lp[ StxJL.UT]< r.h)|| ( lp[ StxJL.m_NRa]+ f< r.h)) sh= StxJL.UT;
        else if(( lp[ StxJL.NRa]< r.h)&& ( lns.sn()!= StxJL.UT)) sh= StxJL.NRa;
        else if( lp[ StxJL.SRa]< r.h) sh= StxJL.SRa;
        if( up( sh)&& dn( lns.sn())) lp[ StxJL.m_NRe]= lns.cn();
        if( r.l< lp[ StxJL.SRa]- 2* f) {
            if( lp[ StxJL.NRe]< r.l) sl= StxJL.SRe;
            else {
                sl= (( r.l< lp[ StxJL.DT])|| ( r.l< lp[ StxJL.m_NRe]- f))?
                    StxJL.DT: StxJL.NRe;
                if( up( lns.sn())) lp[ StxJL.m_NRa]= lns.cn();
            }
        } recDay( ix, sh, sl);
    }

    private void nRa( int ix, float f) {
        StxRec r= data.get( ix); int sh= StxJL.None, sl= StxJL.None;
        if(( lp[ StxJL.UT]< r.h)|| ( lp[ StxJL.m_NRa]+ f< r.h)) sh= StxJL.UT;
        else if( lp[ StxJL.NRa]< r.h) sh= StxJL.NRa;
        if( r.l< lp[ StxJL.NRa]- 2* f) {
            sl= (( lp[ StxJL.NRe]< r.l)? StxJL.SRe:
                 (( r.l< lp[ StxJL.DT]|| r.l< lp[ StxJL.m_NRe]- f)?
                  StxJL.DT: StxJL.NRe));
            if( sl!= StxJL.SRe) lp[ StxJL.m_NRa]= lp[ StxJL.NRa];
        } recDay( ix, sh, sl);
    }

    private void uT( int ix, float f) {
        StxRec r= data.get( ix); int sh= StxJL.None, sl= StxJL.None;
        if( lp[ StxJL.UT]< r.h) sh= StxJL.UT;
        if( r.l<= lp[ StxJL.UT]- 2* f) {
            sl= (( r.l< lp[ StxJL.DT]|| r.l< lp[ StxJL.m_NRe]- f)?
                 StxJL.DT: StxJL.NRe);
            lp[ StxJL.m_NRa]= lp[ StxJL.UT];
        } recDay( ix, sh, sl);
    }

    private void dT( int ix, float f) {
        StxRec r= data.get( ix); int sh= StxJL.None, sl= StxJL.None;
        if( lp[ StxJL.DT]> r.l) sl= StxJL.DT;
        if( r.h>= lp[ StxJL.DT]+ 2* f) {
            sh= (( r.h> lp[ StxJL.UT]|| r.h> lp[ StxJL.m_NRa]+ f)?
                 StxJL.UT: StxJL.NRa);
            lp[ StxJL.m_NRe]= lp[ StxJL.DT];
        } recDay( ix, sh, sl);
    }

    private void nRe( int ix, float f) {
        StxRec r= data.get( ix); int sh= StxJL.None, sl= StxJL.None;
        if(( lp[ StxJL.DT]> r.l)|| ( lp[ StxJL.m_NRe]- f> r.l)) sl= StxJL.DT;
        else if( lp[ StxJL.NRe]> r.l) sl= StxJL.NRe;
        if( r.h> lp[ StxJL.NRe]+ 2* f) {
            sh= (( lp[ StxJL.NRa]> r.h)? StxJL.SRa:
                 (( r.h> lp[ StxJL.UT]|| r.h> lp[ StxJL.m_NRa]+ f)?
                  StxJL.UT: StxJL.NRa));
            if( sh!= StxJL.SRa) lp[ StxJL.m_NRe]= lp[ StxJL.NRe];
        } recDay( ix, sh, sl);
    }

    private void sRe( int ix, float f) {
        StxRec r= data.get( ix); int sh= StxJL.None, sl= StxJL.None;
        if(( lp[ StxJL.DT]> r.l)|| ( lp[ StxJL.m_NRe]- f> r.l)) sl= StxJL.DT;
        else if(( lp[ StxJL.NRe]> r.l)&& ( lns.sn()!= StxJL.DT)) sl= StxJL.NRe;
        else if( lp[ StxJL.SRe]> r.l) sl= StxJL.SRe;
        if( dn( sl)&& up( lns.sn())) lp[ StxJL.m_NRa]= lns.cn();
        if( r.h> lp[ StxJL.SRe]+ 2* f) {
            if( lp[ StxJL.NRa]> r.h) sh= StxJL.SRa;
            else {
                sh= (( r.h> lp[ StxJL.UT])|| ( r.h> lp[ StxJL.m_NRa]+ f))?
                    StxJL.UT: StxJL.NRa;
                if( dn( lns.sn())) lp[ StxJL.m_NRe]= lns.cn();
            }
        } recDay( ix, sh, sl);
    }
    public float getLP( int jl_state) { return lp[ jl_state];}

    private String pf( float f) { 
        return Float.isInfinite(f)|| Float.isNaN(f)? "": String.format( "%.2f", f);
    }

    public String dbgStr() { 
        StringBuilder sb= new StringBuilder();
        if( lns!= null) sb.append( "lns= ").append( lns.dbgStr());
        if( ls!= null) sb.append( "  ls= ").append( ls.dbgStr()).append( "\n");
        sb.append( "SRa=").append( pf( lp[ StxJL.SRa])).
            append( " NRa=").append( pf( lp[ StxJL.NRa])).
            append( " UT=").append( pf( lp[ StxJL.UT])).
            append( " DT=").append( pf( lp[ StxJL.DT])).
            append( " NRe=").append( pf( lp[ StxJL.NRe])).
            append( " SRe=").append( pf( lp[ StxJL.SRe])).
            append( " m_NRa=").append( pf(  lp[ StxJL.m_NRa])).
            append( " m_NRe=").append( pf(lp[ StxJL.m_NRe])).append( "\n");
        return sb.toString();
    }
    public String toString(){
        return stk+ "\n"+ (( recs!= null)? recs.toString(): "");
    }
    public ArrayList<StxJL> data() { return recs.data(); }
    public StxTS<StxRec> stk_data() { return data; }
    public StxRec lastDay() { return  data.get( data.size()- 1); }
    public String lastDayStr() {
        StringBuilder sb= new StringBuilder();
        StxRec r= data.get( data.size()- 1);
        sb.append( String.format( "%s %.2f %.2f %.2f %.2f %.1f [ %.2f]", 
                                  r.date, r.o, r.h, r.l, r.c, vi, 
                                  factorPrice()));
        return  sb.toString();
    }
    public StxJL last() { return  recs.last();}
    public float factorPrice() { return _f;}
    public float avgVol() { return vi;}
    public float avgRg() { return avg_rg;}
    public List<Integer> pivots(int num, String as_of_date) {
        List<Integer> piv = new ArrayList<Integer>(num);
        int ix = 0, ixx = recs.find(as_of_date, -1), xx = recs.get(ixx).xx();
        while((ix < num) && (ixx > 0)) {
            StxJL rec = recs.get(ixx);
            List<Integer> rec_pivs = rec.pxx();
            Integer rec_p = rec_pivs.get(0), rec_p2 = rec_pivs.get(1);
            if ((rec_p2 != null) && (rec_p2 <= xx)) {
//              System.err.printf("P2: %.1f: adding %d to pivs\n", f, -ixx);
                piv.add(0, -ixx); 
                if (++ix >= num)
                    break;
            }
            if ((rec_p != null) && (rec_p <= xx)) {
                ix++;
                piv.add(0, ixx);
//              System.err.printf("P1: %.1f: adding %d to pivs\n", f, ixx);
            }
            --ixx;
        }
        return piv;
    }

    public ArrayList<Integer> pivots(int num) { 
        return pivots(num, false);
    }

    public ArrayList<Integer> pivots(int num, boolean include_lns) {
        ArrayList<Integer> piv = new ArrayList<Integer>(num);
        int ix= 0, ixx= recs.size();
        while((ix < num) && (ixx > 0)) {
            if (recs.get(--ixx).p2) {
                ix++;
                piv.add(0, -ixx);
//              System.err.printf("P2: %.1f: adding %d to pivs\n", f, -ixx);
            }
            if (recs.get(ixx).p) {
                ix++; 
                piv.add(0, ixx); 
//              System.err.printf("P1: %.1f: adding %d to pivs\n", f, ixx);
            }
        }
        if (include_lns) {
            int lns_ix = recs.find(lns.date, 0);
            piv.add(lns_ix * (lns.p? -1: 1));
        }
        return piv;
    }

    public StxData<StxJL> recs() { return recs;}
    public StxJL data( int ix){ return recs.get( ix);}
    public int size() { return recs.size();}

    private void adjustRecordsForSplit(int ix, float split_ratio) {
        for(int ixx = 0; ixx < ix; ixx++)
            recs.get(ixx).split(split_ratio);
    }

    public void setFactor( float f) { this.f= f; }
    public float getFactor() {
        return f;
    }

    public static void main( String [] args) throws Exception {
        String start= "2001-01-01", end= "2013-12-31", stk= null;
        float f= ( float) 1.5; int w= 21, vw= 42; 
        Calendar c= new GregorianCalendar();
        String run_date= String.format( "%d-%02d-%02d", 
                                        c.get( Calendar.YEAR), 
                                        ( 1+ c.get( Calendar.MONTH)),
                                        c.get( Calendar.DAY_OF_MONTH));
        new StxCal( c.get( Calendar.YEAR)+ 1);
        if( args.length< 1) {
            System.err.println( "Please specify stock\n");
            System.exit( 0);
        }
        stk= args[ 0];
        for( int ix= 1; ix< args.length; ix++) {
            if( args[ ix].equals( "-f")&& ++ix< args.length)
                f= Float.parseFloat( args[ ix]);
            else if( args[ ix].equals( "-s")&& ++ix< args.length)
                start= args[ ix];
            else if( args[ ix].equals( "-e")&& ++ix< args.length)
                end= args[ ix];
            else if( args[ ix].equals( "-vw")&& ++ix< args.length)
                vw= Integer.parseInt( args[ ix]);
            else if( args[ ix].equals( "-w")&& ++ix< args.length)
                w= Integer.parseInt( args[ ix]);
            else if( args[ ix].equals( "-date")&& ++ix< args.length)
                run_date= args[ ix];
            else if( args[ ix].equals( "-ten")&& ++ix< args.length) {
                        }
        }
        StxxJL jl= new StxxJL( stk, start, end, f, w, vw, "eod", "split");
        jl.jl( run_date);
        System.err.println( jl.toString());
    }    
}
