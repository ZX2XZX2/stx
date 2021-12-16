package jl;

import core.StxTS;
import core.StxRec;

import java.lang.Math;
import java.util.ArrayList;

public class StxOBV {

    private StxTS<StxRec> data;
    private StxxJL sjl;

    public StxOBV(StxxJL sjl) {
        this.data = sjl.stk_data();
        this.sjl = sjl;
    }

    public StxOBV(StxTS<StxRec> data, StxxJL sjl) {
        this.data = data;
        this.sjl = sjl;
    }

    public float obv(int start_jl, int end) {
//      System.err.printf("%.1f: obv() start = %d end = %d\n", 
//                        sjl.getFactor(), start_jl, end);
        int jls = Math.abs(start_jl);
        float obv = 0;
        StxJL start_rec = sjl.data(jls);
        int start = data.find(start_rec.date, 0);
        StxRec srs = data.get(start), sre = data.get(end);
        int jle = sjl.recs().find(sre.date, 0);
        StxJL end_rec = sjl.data(jle);
        int start_piv_state = (start_jl > 0)? start_rec.s: start_rec.s2;
        if ((sjl.up(start_piv_state) && srs.hiB4Lo()) || 
            (sjl.dn(start_piv_state) && !srs.hiB4Lo()))
            obv += start_rec.obv2 / start_rec.vr;
        obv += start_rec.obv3 / start_rec.vr;
        for(int ix = jls + 1; ix <= jle; ix++) {
            StxJL jlr = sjl.data(ix);
            obv += (jlr.obv1 + jlr.obv2 + jlr.obv3) / jlr.vr;
        }
        return obv;
    }
}
