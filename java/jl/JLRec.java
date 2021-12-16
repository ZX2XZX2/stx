package jl;

import core.StxRecord;

public class JLRec extends StxRecord {

    public float c, c2, vr, arg;
    public int s, s2;
    public boolean p, p2;

    public JLRec( String d, float c, float vr, float arg, int s) {
        super( d, c); this.c= c; this.s= s; this.vr= vr; p= false;
        c2= 0; s2= StxJL.None; p2= false; this.arg= arg;
    }
    public JLRec( String d, float arg) {
        super( d, 0); c= 0; s= StxJL.None; this.vr= 0; p= false;
        c2= 0; s2= StxJL.None; p2= false; this.arg= arg;
    }
    public JLRec( String d, float arg, float vr) {
        super( d, 0); c= 0; s= StxJL.None; this.vr= vr; p= false;
        c2= 0; s2= StxJL.None; p2= false; this.arg= arg;
    }
    public JLRec( JLRec r) {
        super( r.date, 0); c= r.c; s= r.s; vr= r.vr; p= r.p;
        c2= r.c2; s2= r.s2; p2= r.p2; this.arg= r.arg;
    }
    public void split( float s) { c*= s; c2*= s; vr/= s; arg*= s; }
    public void dividend( float d) { c-= d; c2-= d; }

    public float c(){ return( c2== 0)? c: c2; }
    public void c( float ic){ if( c== 0) c= ic; else c2= ic; }
    public int s(){ return( c2== 0)? s: s2; }
    public void s( int is){ if( c== 0) s= is; else s2= is;}
    public float arg() { return arg; }
//     public void s( int is){ System.err.print( "Setting state for date "+ date); if( c== 0) { s= is; System.err.println( " s= "+ is);} else{ s2= is; System.err.println( " s2= "+ is);}}

    public boolean p(){ return( c2== 0)? p: p2; }
    public void p( boolean ip){ if( c2== 0) p= ip; else p2= ip; }

    public String toString() {
        return ( s== StxJL.None)? String.format( "%9s\n", date):
            String.format( "%9s %"+ ( 10+ 10* s)+ ".2f 111\n", date, c);
    }
    public String dbgStr() {
        StringBuilder sb= new StringBuilder();
        sb.append( date).append( " ").append( c).append( " ").
            append( state( s)).append( " ").append( p).append( " ||||| ").
            append( c2).append( " ").append( state( s2)).append( " ").
            append( p2);
        return sb.toString();
    }
    String state( int st) {
        switch( st) {
        case StxJL.SRa: return "SRa"; case StxJL.NRa: return "NRa";
        case StxJL.UT:  return "UT"; case StxJL.DT:  return "DT";
        case StxJL.NRe: return "NRe"; case StxJL.SRe: return "SRe";
        }
        return "None";
    }
    public static void main( String[] args) {
//         JLRec jlr= new JLRec( "28-Dec-09", 50, 0, StxJL.SRa);
//         System.err.println( jlr.toString());
//         jlr= new JLRec( "28-Dec-09", 50, 0, StxJL.NRa);
//         System.err.println( jlr.toString());
//         jlr= new JLRec( "28-Dec-09", 50, 0, StxJL.UT);
//         System.err.println( jlr.toString());
//         jlr= new JLRec( "28-Dec-09", 50, 0, StxJL.DT);
//         System.err.println( jlr.toString());
//         jlr= new JLRec( "28-Dec-09", 50, 0, StxJL.NRe);
//         System.err.println( jlr.toString());
//         jlr= new JLRec( "28-Dec-09", 50, 0, StxJL.SRe);
//         System.err.println( jlr.toString());
    }
}
