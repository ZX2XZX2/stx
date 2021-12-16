package core;

public class StxRec extends StxRecord {

    public float o, h, l, c, v;
    public static String sep= " ";

    public StxRec() {}
    public StxRec( String line) throws Exception {
        super( line);
        en= line.charAt( 0);
        if(( en== '*')|| ( en== 'D')) line= line.substring( 1); 
        else en= '\0';
        String [] toks= line.split( sep);
        if( toks.length!= 6+ (( en!= '\0')? 1: 0))
            throw new Exception( "Wrong line: "+ line);
        date= toks[ 0]; o= Float.parseFloat( toks[ 1]);
        h= Float.parseFloat( toks[ 2]); l= Float.parseFloat( toks[ 3]);
        c= Float.parseFloat( toks[ 4]); v= Float.parseFloat( toks[ 5]);
        if( en!= '\0') { ev= Float.parseFloat( toks[ 6]); ef= true;}
    }
    public StxRec( StxRec sr) {
        super( sr); o= sr.o; h= sr.h; l= sr.l; c= sr.c; v= sr.v;
    }
    public StxRec( String date, float val) {
        super( date, val); o= val; h= val; l= val; c= val; v= 0;
    }
    public StxRec( String date, float o, float h, float l, float c) {
        super( date, c); this.o= o; this.h= h; this.l= l; this.c= c; }
    public StxRec( String date, float o, float h, float l, float c, float v) {
        super( date, c); this.o= o; this.h= h; this.l= l; this.c= c; this.v= v;}

    public StxRec(String date, int o, int h, int l, int c, int v) {
        super(date, (float)(c / 100.0));
        this.o = (float)(o / 100.0);
        this.h = (float)(h / 100.0);
        this.l = (float)(l / 100.0);
        this.c = (float)(c / 100.0);
        this.v= (float) v;
    }
    
    public void copySR( StxRec sr) {
        this.date= sr.date; this.en= sr.en; this.ev= sr.ev; this.ef= sr.ef;
        this.o= sr.o; this.h= sr.h; this.l= sr.l; this.c= sr.c; this.v= sr.v;
    }
    public void split( float s) { o*= s; h*= s; l*= s; c*= s; v/= s; }
    public void split2( float s) { o*= s; h*= s; l*= s; c*= s; v*= s; }
    public void dividend( float d) { o-= d; h-= d; l-= d; c-= d; }
    public String toString() {
        return( !ef? String.format( "%9s %.2f %.2f %.2f %.2f %.0f\n",
                                    date, o, h, l, c, v):
                String.format( "%c%9s %.2f %.2f %.2f %.2f %.0f %.2f\n",
                               en, date, o, h, l, c, v, ev));
    }
    public String toString1() {
        return( !ef? String.format( "%9s %.2f %.2f %.2f %.2f %.0f",
                                    date, o, h, l, c, v):
                String.format( "%c%9s %.2f %.2f %.2f %.2f %.0f %.2f",
                               en, date, o, h, l, c, v, ev));
    }
    public float wPrice() { return ( h+ l+ c)/ 3;}
    public float activity() { return v* ( h+ l+ c)/ 3;}
    public boolean hiB4Lo() { return 2* c< ( h+ l);}
    public float trueRange( StxRec prev) {
        float tr= h- l; if( prev== null) return tr;
        float diff= Math.abs( h- prev.c); if( tr< diff) tr= diff;
        diff= Math.abs( l- prev.c); if( tr< diff) tr= diff;
        return tr;
    }
    public int gap( StxRec sr_1) {
        if( sr_1== null) return 0;
        if( h< sr_1.l) return -1;
        if( l> sr_1.h) return 1;
        return 0;
    }
    public boolean longBody( float lb) { return Math.abs( c- o)>= lb; }
    public boolean shortBody( float sb) { return Math.abs( c- o)<= sb; }
    public boolean longUpperShadow( float ls) {
        return((( o< c)? ( h- c): ( h- o))>= ls);
    }
    public boolean shortUpperShadow( float ss) {
        return ((( o< c)? ( h- c): ( h- o))<= ss);
    }
    public boolean longLowerShadow( float ls) {
        return ((( o< c)? ( o- l): ( c- l))>= ls);
    }
    public boolean shortLowerShadow( float ss) {
        return ((( o< c)? ( o- l): ( c- l))<= ss);
    }
    public boolean hammer( float sb) {
        return( shortBody( sb)&& (( h- (( o< c)? o: c))< ( h- l)/ 3));
    }
    public boolean shootingStar( float sb) {
        return( shortBody( sb)&& (((( o> c)? o: c)- l)< ( h- l)/ 3));
    }
    public boolean bullishBelt( float lb, float lss, float uss) {
        return(( o< c)&& longBody( lb)&& shortLowerShadow( lss)&&
               shortUpperShadow( uss));
    }
    public boolean bearishBelt( float lb, float lss, float uss) {
        return(( o> c)&& longBody( lb)&& shortLowerShadow( lss)&&
               shortUpperShadow( uss));
    }
    public boolean highWave( float bs, float ls) {
        return( shortBody( bs)&& longUpperShadow( ls)&&
                longLowerShadow( ls));
    }
    public boolean darkCloudCover( StxRec sr_1, float bl, float ss) {
        if( sr_1== null) return false;
        return(( o> c)&& ( o> sr_1.h)&& ( sr_1.o< sr_1.c)&&
               ( c< 0.5* ( sr_1.o+ sr_1.c))&& sr_1.longBody( bl)&&
               sr_1.shortUpperShadow( ss));
    }
    public boolean piercing( StxRec sr_1, float bl, float ss) {
        if( sr_1== null) return false;
        return(( o< c)&& ( o< sr_1.l)&& ( sr_1.o> sr_1.c)&&
               ( c> ( sr_1.o+ sr_1.c)/ 2)&& sr_1.longBody( bl)&&
               sr_1.shortLowerShadow( ss));
    }
    public boolean bearishEngulfing( StxRec sr_1) {
        if( sr_1== null) return false;
        return(( sr_1.o< sr_1.c)&& ( o> c)&& ( o> sr_1.c)&& ( c< sr_1.o));
    }
    public boolean bullishEngulfing( StxRec sr_1) {
        if( sr_1== null) return false;
        return(( sr_1.o> sr_1.c)&& ( o< c)&& ( o< sr_1.c)&& ( c> sr_1.o));
    }
    public boolean harami( StxRec sr_1, float bl, float bs, int trnd) {
        if( sr_1== null) return false;
        float oc_max= ( o> c)? o: c, oc_min= ( o< c)? o: c;
        if( trnd== 1)
            return(( sr_1.o< sr_1.c)&& sr_1.longBody( bl)&&
                   shortBody( bs)&& ( sr_1.c> oc_max)&& ( sr_1.o< oc_min));
        return(( sr_1.o> sr_1.c)&& sr_1.longBody( bl)&&
               shortBody( bs)&& ( sr_1.o> oc_max)&& ( sr_1.c< oc_min));
    }
    public boolean preEveningStar( StxRec sr_1, float bl, float bs) {
        if( sr_1== null) return false;
        return(( sr_1.o< sr_1.c)&& sr_1.longBody( bl)&&
               ( o> sr_1.c)&& shortBody( bs)&& ( c> sr_1.c));
    }
    public boolean postEveningStar( StxRec sr_2, StxRec sr_1,
                                    float bl, float bs) {
        if(( sr_2== null)|| ( sr_1== null)) return false;
        return(( o> c)&& ( c< 0.5* ( sr_2.o+ sr_2.c)));
    }
    public boolean eveningStar( StxRec sr_2, StxRec sr_1,
                                float bl, float bs) {
        if(( sr_2== null)|| ( sr_1== null)) return false;
        return(( sr_2.o< sr_2.c)&& sr_2.longBody( bl)&&
               ( sr_1.o> sr_2.c)&& sr_1.shortBody( bs)&& ( sr_1.c> sr_2.c)&&
               ( o> c)&& ( c< 0.5* ( sr_2.o+ sr_2.c)));
    }
    public boolean preMorningStar( StxRec sr_1, float bl, float bs) {
        if( sr_1== null) return false;
        return(( sr_1.o> sr_1.c)&& sr_1.longBody( bl)&&
               ( o< sr_1.c)&& shortBody( bs)&& ( c< sr_1.c));
    }
    public boolean postMorningStar( StxRec sr_2, StxRec sr_1,
                                    float bl, float bs) {
        if(( sr_2== null)|| ( sr_1== null)) return false;
        return(( c> o)&& ( c> 0.5* ( sr_2.o+ sr_2.c)));
    }
    public boolean morningStar( StxRec sr_2, StxRec sr_1,
                                float bl, float bs) {
        if(( sr_2== null)|| ( sr_1== null)) return false;
        return(( sr_2.o> sr_2.c)&& sr_2.longBody( bl)&&
               ( sr_1.o< sr_2.c)&& sr_1.shortBody( bs)&& ( sr_1.c< sr_2.c)&&
               ( c> o)&& ( c> 0.5* ( sr_2.o+ sr_2.c)));
    }
}
