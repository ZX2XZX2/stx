package indicators;

import core.StxData;
import core.StxRec;

import java.util.List;

public class StxHL<T extends StxRec> {

    public float hi, lo; 
    public int d_hi, d_lo;
    private StxData<T> data;
    private int hld;

    public StxHL( StxData<T> data, int hld) {
        this.data= data; this.hld= hld;
    }
    public StxHL() {}
    public void init() {
        d_hi= -1; d_lo= -1;
        hi= Float.NEGATIVE_INFINITY; lo= Float.POSITIVE_INFINITY;
    }

    public void hl( List<T> ts_data, int hld, int ix) {
        init();
        T r= null; int x= ix- hld+ 1; if( x< 0) x= 0;
        while( x<= ix) {
            r= ts_data.get( x); if( r.h> hi) { hi= r.h; d_hi= x;}
            if( r.l< lo) { lo= r.l; d_lo= x;} x++;
        }
    }

    public void hl( int ix) {
        d_hi= -1; d_lo= -1;
        hi= Float.NEGATIVE_INFINITY; lo= Float.POSITIVE_INFINITY;
        T r= null; 
        int x= ix- hld+ 1; if( x< 0) x= 0;
        while( x<= ix) {
            r= data.get( x); if( r.h> hi) { hi= r.h; d_hi= x;}
            if( r.l< lo) { lo= r.l; d_lo= x;} x++;
        }
    }
    public String toString() {
        return String.format( "hi= %.2f, d_hi= %d, lo= %.2f, d_lo= %d\n hi_rec= %s lo_rec= %s",
                              hi, d_hi, lo, d_lo, data.get( d_hi).toString(), data.get( d_lo).toString());
    }
}
