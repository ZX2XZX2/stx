package core;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;

public class StxCal {

    class Holiday implements Comparable<Holiday> {
        public Holiday( String str, String des) {
            this.str= str; this.des= des; num= StxCal.numDate( str);
        }
        public Holiday( int num, String des) {
            this.num= num; this.des= des; str= StxCal.strDate( num);
        }
        public boolean equals( Holiday h) { return( num== h.num());}
        public boolean equals( Object o) { 
            return( num== (( Holiday) o).num());
        }
        int num() { return num;}
        public int compareTo( Holiday h) {
            if( num== h.num()) return 0; if( num< h.num()) return -1;
            return 1;
        }
        public String toString() { return str+ " "+ des;}
        private String str, des;
        private int num;
    };

    private static int s_year;
    private static ArrayList<Holiday> holidays;
    private static HashMap<Integer, Integer> hol_map;
    private static HashMap<String, Integer> ymtnd;
    private static int dow_1;

    static private int easterMon[] = {
        107,  98,  90, 103,  95, 114, 106,  91, 111, 102,   // 1900-1909
        87, 107,  99,  83, 103,  95, 115,  99,  91, 111,   // 1910-1919
        96,  87, 107,  92, 112, 103,  95, 108, 100,  91,   // 1920-1929
        111,  96,  88, 107,  92, 112, 104,  88, 108, 100,   // 1930-1939
        85, 104,  96, 116, 101,  92, 112,  97,  89, 108,   // 1940-1949
        100,  85, 105,  96, 109, 101,  93, 112,  97,  89,   // 1950-1959
        109,  93, 113, 105,  90, 109, 101,  86, 106,  97,   // 1960-1969
        89, 102,  94, 113, 105,  90, 110, 101,  86, 106,   // 1970-1979
        98, 110, 102,  94, 114,  98,  90, 110,  95,  86,   // 1980-1989
        106,  91, 111, 102,  94, 107,  99,  90, 103,  95,   // 1990-1999
        115, 106,  91, 111, 103,  87, 107,  99,  84, 103,   // 2000-2009
        95, 115, 100,  91, 111,  96,  88, 107,  92, 112,   // 2010-2019
        104,  95, 108, 100,  92, 111,  96,  88, 108,  92,   // 2020-2029
        112, 104,  89, 108, 100,  85, 105,  96, 116, 101,   // 2030-2039
        93, 112,  97,  89, 109, 100,  85, 105,  97, 109,   // 2040-2049
        101,  93, 113,  97,  89, 109,  94, 113, 105,  90,   // 2050-2059
        110, 101,  86, 106,  98,  89, 102,  94, 114, 105,   // 2060-2069
        90, 110, 102,  86, 106,  98, 111, 102,  94, 107,   // 2070-2079
        99,  90, 110,  95,  87, 106,  91, 111, 103,  94,   // 2080-2089
        107,  99,  91, 103,  95, 115, 107,  91, 111, 103    // 2090-2099
    };
    static private String m_names[] = {
        "", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep",
        "Oct", "Nov", "Dec"
    };
    public StxCal( int e_year) {
        s_year= 1901;
        int ix= 1, y= s_year- 1; dow_1= 0; 
        ymtnd= new HashMap<String, Integer>();
        while( y++<= e_year) {
            ymtnd.put( y+ "-01", ix); ix+= 31;
            ymtnd.put( y+ "-02", ix);
            ix+= (((( y% 400)== 0)|| (( y% 100!= 0)&&
                                      ( y% 4== 0)))? 29: 28);
            ymtnd.put( y+ "-03", ix); ix+= 31;
            ymtnd.put( y+ "-04", ix); ix+= 30;
            ymtnd.put( y+ "-05", ix); ix+= 31;
            ymtnd.put( y+ "-06", ix); ix+= 30;
            ymtnd.put( y+ "-07", ix); ix+= 31;
            ymtnd.put( y+ "-08", ix); ix+= 31;
            ymtnd.put( y+ "-09", ix); ix+= 30;
            ymtnd.put( y+ "-10", ix); ix+= 31;
            ymtnd.put( y+ "-11", ix); ix+= 30;
            ymtnd.put( y+ "-12", ix); ix+= 31;
        } ymtnd.put( y+ "-01", ix);
        holidays= new ArrayList<Holiday>();
        hol_map= new HashMap<Integer, Integer>();
        for( y= s_year; y<= e_year; y++) addHols( y); y= e_year+ 1; 
        String d= strDate( getYmtnd( y, "-01")+ easterMon[ y- 1900]- 3);
        addHol( y, d.substring( 4), "GoodFriday"); ix= 0; 
        Collections.<Holiday>sort( holidays);
        for( Holiday h: holidays) hol_map.put( h.num(), ix++);
    }
    static public int numDate( String s) {        
        return( ymtnd.get( s.substring( 0, 7))+
                Integer.parseInt( s.substring( 8)));
    }
    static private int getYmtnd( int y, String m) {
        StringBuilder sb= new StringBuilder();
        return ymtnd.get( sb.append( y).append( m).toString());
    }
    static public String strDate( int num) {
        int y= s_year+ num/ 365, m= 1, d= 0, ndy= getYmtnd( y, "-01");
        if( ndy>= num) ndy= getYmtnd( --y, "-01"); 
        m+= ( num- ndy)/ 30; if( m> 12) m= 12;
        int ndm= getYmtnd( y, String.format( "-%02d", m));
        if( ndm>= num) { if( --m< 1) m= 12;
            ndm= getYmtnd( y, String.format( "-%02d", m)); }
        d+= ( num- ndm);
        return String.format( "%d-%02d-%02d", y, m, d);
    }
    static public int dow( String s) { return( numDate( s)- dow_1)% 7;} 
    static private int dow( int n) { return( n- dow_1)% 7; } 
    static public String sow( String d) {
        if( d.equals( "2001-09-17")) return d;
        if( isBusDay( d)== false) d= prevBusDay( d);
        int dow= dow( d), dow1= dow+ 1;
        while(( dow>= 1)&& ( dow1> dow)) {
            dow1= dow; d= prevBusDay( d); dow= dow( d);
        }
        if( dow== 1) return d;
        return nextBusDay( d);
    }
    static public boolean isBusDay( String s) { 
        return isBusDay( numDate( s));
    }
    static public boolean isBusDay( int n) { int d= dow( n);
        return( d!= 6&& d!= 0&& !hol_map.containsKey( n));
    }
    static public String nextBusDay( String s) { 
        return strDate( nextBusDay( numDate( s)));
    }
    static private int nextBusDay( int n) { 
        int d= 1; while( d> 0) if( isBusDay( ++n)) d--; return n;
    }
    static public String prevBusDay( String s) { 
        return strDate( prevBusDay( numDate( s)));
    }
    static private int prevBusDay( int n) { 
        int d= 1; while( d> 0) if( isBusDay( --n)) d--; return n;
    }
    static public String moveBusDays( String s_date, int days) {
        if( days== 0) return s_date;
        String e_date= moveWeekDays( s_date, days);
        int num_hols= numHols( s_date, e_date);
        if( num_hols== 0) return e_date;
        return moveBusDays( e_date, num_hols);
    }
    static public String moveWeekDays( String s, int d) { 
        int n= numDate( s), dw, nw= d/ 7; d-= 5* nw; n+= 7* nw;
        while( d> 0) { n++; dw= dow( n); if( dw!= 6&& dw!= 0) d--;}
        while( d< 0) { n--; dw= dow( n); if( dw!= 6&& dw!= 0) d++;}
        return strDate( n);
    }
    static String moveDays( String d, int n) { return strDate( numDate( d)+ n);}
    int moveDays( int d, int n) { return d+ n;}
    public static String moveToBusDay( String d, int n) {
        return( isBusDay( d)? d: (( n== 1)? nextBusDay( d): prevBusDay( d)));
    }
    public static int year( String d) {
        return Integer.parseInt( d.substring( 0, d.indexOf( "-")));
    }
    static public int numHols( String d1, String d2) {
        int n1= numDate( d1), n2= numDate( d2), sign= ( n1< n2)? 1: -1;
        String s= ( n1< n2)? d1: d2, e= ( n1< n2)? d2: d1;
        int sn= numDate( s), en= numDate( e), ys= year( s), ye= year( e);
        int sh= getYmtnd( ys, "-01")+ easterMon[ ys- 1900]- 3; ys--;
        if(ys == 1900) 
            sh = 2;
        else {
            if( sh> sn) sh= getYmtnd( ys, "-01")+ easterMon[ ys- 1900]- 3;
        }
        int eh= getYmtnd( ye, "-01")+ easterMon[ ye- 1900]- 3; ye++;
        if( eh< en) eh= getYmtnd( ye, "-01")+ easterMon[ ye- 1900]- 3;
        int ix1= hol_map.get( sh), ix2= hol_map.get( eh);
        while( holidays.get( ix1).num()< sn) ix1++;
        while( holidays.get( ix2).num()> en) ix2--;
        return sign* ( 1+ ix2- ix1);
    }
    static public int numWeekDays( String s_date, String e_date) {
        int s= numDate( s_date), e= numDate( e_date), d, num, sgn= 1;
        d= e- s; if( d< 0) sgn= -1;  num= 5* ( d/ 7); s+= sgn* 7*( d/ 7);
        while( s!= e) { if( dow( s)!= 6&& dow( s)!= 0) num++; s+= sgn; }
        return sgn* num;
    }
    static public int numBusDays( String s_date, String e_date) {
        return numWeekDays( s_date, e_date)- numHols( s_date, e_date);
    }
    static public int numBusDaysExpiry( String s_date, String exp_date) {
        int res = numBusDays(s_date, exp_date);
        return (StxCal.cmp(exp_date, "2015-01-17") > 0)? res - 1: res;
    }
    private void addHol( int y, String md, String des, int dw, int wn) {
        StringBuilder sb= new StringBuilder(); sb.append( y).append( md);
        String s= sb.toString();
        int n= numDate( s)+ ( 7* ( wn- 1)+ ( dw- dow( s)+ 7)% 7);
        holidays.add( new Holiday( n, des));
    }
    private void addHol( int y, String md, String des) {
        StringBuilder sb= new StringBuilder(); sb.append( y).append( md);
        String s= sb.toString(); boolean b= md.equals( "-01-01"); 
        int dw= dow( s); if( dw== 0) s= moveWeekDays( s, 1);
        else if( dw== 6){ if( b) return; else s= moveWeekDays( s, -1);}
        holidays.add( new Holiday( s, des));
    }
    private void addHols( int y) { String des, s;
        addHol( y, "-01-01", "New Year");
        if( y== 2007) addHol( y, "-01-02", "Ford's Funeral");
        if( y>= 1998) addHol(  y, "-01-01", "ML King Day", 1, 3);
        if( y== 1973) addHol( y, "-01-25", "Johnson's funeral");
        if( y== 1969) addHol( y, "-02-10", "Heavy snow");
        if( y< 1971) addHol( y, "-02-22", "Presidents Day");
        if( y>= 1971) addHol( y, "-02-01", "Presidents Day", 1, 3);
        if( y== 1969) addHol( y, "-03-31", "Eisenhower's funeral");
        s= strDate( getYmtnd( y, "-01")+ easterMon[ y- 1900]- 3);
        addHol( y, s.substring(  4), "Good Friday");
        if( y== 1994) addHol( y, "-04-27", "Nixon's funeral");
        if( y< 1970) addHol( y, "-05-30", "Memorial Day");
        if( y>= 1971) addHol( y, "-05-25", "Memorial Day", 1, 1);
        if( y== 2004) addHol( y, "-06-11", "Reagan's funeral");
        addHol( y, "-07-04", "Independence Day");
        if( y== 1968)
            addHol( y, "-07-05", "Day after Independence Day");
        if( y== 1977) addHol( y, "-07-14", "1977 Blackout");
        if( y== 1969) addHol( y, "-07-21", "Lunar Exploration");
        addHol( y, "-09-01", "Labor Day", 1, 1);
        if( y== 2001) { des= "September 11, 2001";
            addHol( y, "-09-11", des); addHol( y, "-09-12", des); 
            addHol( y, "-09-13", des); addHol( y, "-09-14", des);
        }
        if(( y<= 1980)&& (( y% 4)== 0))
            addHol( y, "-11-01", "Pres. Election Day", 2, 1);
        addHol( y, "-11-01", "Thanksgiving Day", 4, 4);
        addHol( y, "-12-25", "Christmas Day");
        if( y == 1972) addHol( y, "-12-28", "Truman's funeral");
        if( y== 1968) { des= "Paperwork Crisis";
            addHol( y, "-06-12", des);
            int ix= numDate( "1968-06-12"), e= numDate( "1968-10-30");
            while( ix<= e) { ix+= 7; s= strDate( ix);
                if( !s.equals( "1968-07-03")&& !s.equals( "1968-09-04")&&
                    !s.equals( "1968-11-06")&& !s.equals( ""))
                    addHol( y, s.substring( 4), des);
            }
            addHol( y, "-11-11", des);
            addHol( y, "-11-20", des); addHol( y, "-12-04", des);
            addHol( y, "-12-11", des); addHol( y, "-12-18", des);
        }
        if( y== 1985) addHol( y, "-09-27", "1985 Whatever");
        if( y== 2012) {
            addHol( y, "-10-29", "Frankenstorm 1");
            addHol( y, "-10-30", "Frankenstorm 2");
        }
        if( y== 2018) addHol( y, "-12-05", "George H.W. Bush funeral");

    }
    public void printHolidays() {
        for( Holiday h: holidays) System.err.println( h.toString());
    }
    public void printHoliday( int nhd) {
        Integer h= hol_map.get( nhd);
        if( h!= null) System.err.println( holidays.get( h).toString());
    }
    static public int cmp( String d, String e) {
        int m= numDate( d), n= numDate( e); return( m> n)? -1: (( m< n)? 1: 0);
    }
    static public int month( String d) { 
        String [] tks= d.split( "-"); return Integer.parseInt( tks[ 1]);
    }
    static public int day( String d) { 
        String [] tks= d.split( "-"); return Integer.parseInt( tks[ 2]);
    }
    static public String ymd2idd( String ymd) {
        String [] tks= ymd.split( "-");
        return String.format( "%s%02d%02d", tks[ 0], Integer.parseInt( tks[ 1]),
                              Integer.parseInt( tks[ 2]));
    }
    static public String idd2ymd( String idd) {
        return String.format( "%d-%02d-%02d",
                              Integer.parseInt( idd.substring( 0, 4)),
                              Integer.parseInt( idd.substring( 4, 6)),
                              Integer.parseInt( idd.substring( 6, 8)));
    }
    static public int idd2hm( String idd) {
        return( isBusDay( StxCal.idd2ymd( idd))?
                100* Integer.parseInt( idd.substring( 8, 10))+
                Integer.parseInt( idd.substring( 10, 12)): 0);
    }
    static public String idd2jl( String idd) {
        return ( m_names[ Integer.parseInt( idd.substring( 4, 6))]+ " "+
                 idd.substring(6, 8)+ " "+ idd.substring( 8, 10)+ ":"+
                 idd.substring( 10, 12));
    }
    static public String nextIddBusDay( String d) {
        String[] t= nextBusDay( d).split( "-"); return t[ 0]+ t[ 1]+ t[ 2];
    }
    static public String prevIddBusDay( String d) {
        String[] t= prevBusDay( d).split( "-"); return t[ 0]+ t[ 1]+ t[ 2];
    }
    static public String buildIdd( String ymd, int h, int m) {
        return ymd+ String.format( "%02d%02d", h, m);
    }
    static public String advanceID( String idd) { return advanceID( idd, 5);}
    static public String advanceID( String idd, int mins) {
        int hm= idd2hm( idd), h= hm/ 100, m= hm% 100+ mins;
        if( hm== 0) return buildIdd( nextIddBusDay( idd2ymd( idd)), 9, 30);
        if( m>= 60) { 
            if( h== 15) return buildIdd( nextIddBusDay( idd2ymd( idd)), 9, 30);
            else { ++h; m= m% 60; }
        } else return idd.substring( 0, 10)+ String.format( "%02d", m);
        return buildIdd( idd.substring( 0, 8), h, m);
    }
    static public String retreatID( String idd) { return retreatID( idd, 5); }
    static public String retreatID( String idd, int mins) {
        int hm= idd2hm( idd), h= hm/ 100, m= hm% 100- mins;
        if( hm== 0)
            return buildIdd( prevIddBusDay( idd2ymd( idd)), 15, 60- mins);
        if( m< 0) {
            --h; m= 60- mins; return buildIdd( idd.substring( 0, 8), h, m);
        } else if(( h== 9)&& ( m== 30- mins))
            return buildIdd( prevIddBusDay( idd2ymd( idd)), 15, 60- mins);
        else return idd.substring( 0, 10)+ String.format( "%02d", m);
    }

    static public boolean isBD( String ed) {
        String[] tokens= ed.split( " ");
        return isBusDay( tokens[ 0]);
    }

    static public String prevBD( String ed) {
        String[] tokens= ed.split( " ");
        if( tokens[ 1].compareTo( "09:30")== 0)
            ed= prevBusDay( tokens[ 0])+ " "+ tokens[ 1];
        else
            ed= tokens[ 0]+ " 09:30";
        return ed;
    }
    static public String nextBD( String ed) {
        String[] tokens= ed.split( " ");
        if( tokens[ 1].compareTo( "15:55")== 0)
            ed= nextBusDay( tokens[ 0])+ " "+ tokens[ 1];
        else
            ed= tokens[ 0]+ " 15:55";
        return ed;
    }

    static public String gui2ymd( String ed) {
        return ed.substring( 0, ed.indexOf( " "));
    }
    static public String gui2idd( String ed) {
        String[] tokens= ed.split( " ");
        String[] td= tokens[ 0].split( "-");
        String[] thm= tokens[ 1].split( ":");
        return td[ 0]+ td[ 1]+ td[ 2]+ thm[ 0]+ thm[ 1];
    }
    static public String idd2gui( String idd) {
        int hhmm= idd2hm( idd);
        return idd2ymd( idd)+ " "+
            String.format( "%02d:%02d", hhmm/ 100, hhmm% 100);
    }
    static public String us2ymd( String us_date) {
        String[] ymd= us_date.split( "/"); String res= null;
        try {
            int mm= Integer.parseInt( ymd[ 0]);
            int dd= Integer.parseInt( ymd[ 1]);
            int yy= Integer.parseInt( ymd[ 2]);
            res= String.format( "%d-%02d-%02d", yy, mm, dd);
        } catch( Exception ex) { res= null;}
        return res;
    }
    static public double numBusDaysID( String s_date, String e_date) {
        String sd= idd2ymd( s_date), ed= idd2ymd( e_date);
        int num_days= 1+ numBusDays( sd, ed);
        int hhmm1= StxCal.idd2hm( s_date), hhmm2= StxCal.idd2hm( e_date);
        return num_days- (( hhmm1/ 100 - 9)* 60+ hhmm1% 100- 30+
                          ( 15 - hhmm2/ 100)+ 59- hhmm2% 100)/ 390.0; 
    }
    static public String hhmm( String idd) {
        return idd.substring( idd.length()- 4);
    }
    static public String advanceID1( String ed) {
        String idd= advanceID( gui2idd( ed));
        return idd2gui( idd);
    }
    static public String retreatID1( String ed) { 
        String idd= retreatID( gui2idd( ed));
        return idd2gui( idd);
    }
    static public String advanceIDay( String ed) {
        String idd= gui2idd( ed);
        int hm= idd2hm( idd), h= hm/ 100, m= hm% 100;
        return buildIdd( nextIddBusDay( idd2ymd( idd)), h, m);
    }
    static public String retreatIDay( String ed) { 
        String idd= gui2idd( ed);
        int hm= idd2hm( idd), h= hm/ 100, m= hm% 100;
        return buildIdd( prevIddBusDay( idd2ymd( idd)), h, m);
    }
    static public String subtractMins( String idd, int mins) {
        String hhmm= StxCal.hhmm( idd);
        String ymd= idd.substring( 0, idd.length()- 4);
        int hh= Integer.parseInt( hhmm.substring( 0, 2));
        int mm= Integer.parseInt( hhmm.substring( 2, 4));
        if( mm< mins) { --hh; mm+= ( 60- mins); }
        else mm-= mins;
        return ymd+ String.format( "%02d%02d", hh, mm);
    }
    static public ArrayList<String> getJLIDates( String idd, int gran, int mins){
        int hhmm= idd2hm( idd), hh= hhmm/ 100, mm= hhmm% 100, hh1= hh, mm1= mm; 
        int daily_mins=(hh-9)*60+(mm-30); daily_mins-=(daily_mins%gran);
        if( daily_mins< 30) { hh= 9; mm= 30+ daily_mins; }
        else { daily_mins-= 30; hh= 10+ daily_mins/ 60; mm= daily_mins% 60;}
        ArrayList<String> jlid_dates= new ArrayList<String>();
        jlid_dates.add(String.format("%s%02d%02d",idd.substring(0,8),hh,mm));
        mm1+= ( mins- 1); if( mm1> 60) { mm1-= 60; ++hh1;}
        jlid_dates.add(String.format("%s%02d%02d",idd.substring(0,8),hh1,mm1));
        return jlid_dates;
    }
    static public boolean lastInCycle( String idd, int mins) {
        int hhmm= idd2hm( idd), hh= hhmm/ 100, mm= hhmm% 100; 
        int daily_mins= ( hh- 9)* 60+ ( mm- 30);
        return (( daily_mins+ 1)% mins== 0);            
    }
    static public String getDailyExpiration( String date) {
        int dwk= dow( date);
        if(( dwk!= 4)&& ( dwk!= 5)) return null;
        if( dwk== 5) return isBusDay( date)? date: null;
        String exp_date= moveDays( date, 1);
        if( !isBusDay( exp_date)) exp_date= prevBusDay( exp_date);
        return exp_date;
    }
    static public String getWeeklyExpiration( String date) {
        int dwk= dow( date);
        String exp_date= moveWeekDays( date, -dwk+ (( dwk< 4)? 5: 10));
        int wk_number= 1+ Integer.parseInt( exp_date.substring( 8))/ 7;
        if( wk_number== 3)
            exp_date= moveDays( exp_date, 1);
        else
            if( !isBusDay( exp_date)) exp_date= prevBusDay( exp_date);
        return exp_date;
    }
    static public String getMonthlyExpiration( String date) {
        int dwk= dow( date);
        String exp_date= moveWeekDays( date, 5- dwk);
        int week_num = 1 + (Integer.parseInt(exp_date.substring(8)) - 1) / 7;
        if(week_num < 3)
            exp_date= moveWeekDays( exp_date, 5* ( 3- week_num));
        else if( week_num> 3) {
            while( month( date)== month( exp_date))
                exp_date= moveWeekDays( exp_date, 5);
            exp_date= moveWeekDays( exp_date, 10);
        }
        if( cmp( exp_date, "2015-01-17")>= 0)
            exp_date= moveDays( exp_date, 1);
        else {
            if( !isBusDay( exp_date)) exp_date= prevBusDay( exp_date);
        }
        return exp_date;
    }
    static public String getMonthlyExpiration(String date, int num_months) {
        String exp_date = getMonthlyExpiration(date);
        boolean one_month = ( num_months == 1);
        while(num_months > 1) {
            exp_date = moveWeekDays(exp_date, 10);
            exp_date = getMonthlyExpiration(exp_date);
            --num_months;
        }
        // if(one_month == false && (cmp(exp_date, "2015-01-17") >= 0))
        //     exp_date = StxCal.moveDays(exp_date, 1);
        return exp_date;
    }
    static public String getMonthlyExpiration( String date, int num_months,
                                               int min_days) {
        String next_exp_date= getMonthlyExpiration( date);
        if( numBusDays( date, next_exp_date)< min_days)
            return getMonthlyExpiration( date, num_months+ 1);
        if( num_months== 1)
            return next_exp_date;
        return getMonthlyExpiration( date, num_months);
    }

    static public List<String> expiries( String sd, String ed,
                                         SimpleDateFormat ymd)
        throws ParseException {
        return expiries( sd, ed, ymd, 0);
    }

    static public List<String> expiries( String sd, String ed,
                                         SimpleDateFormat ymd, int m)
        throws ParseException {
        List<String> res= new ArrayList<String>();
        Calendar start= new GregorianCalendar();
        start.setTime( ymd.parse( sd));
        start.set( Calendar.DAY_OF_MONTH, 1);
        start.add( Calendar.MONTH, m);
        Calendar end= new GregorianCalendar();
        end.setTime( ymd.parse( ed));
        end.set( Calendar.DAY_OF_MONTH, 1);
        end.add( Calendar.MONTH, 1);
        while( start.compareTo( end)<= 0) {
            String dt= moveToBusDay( ymd.format( start.getTime()), 1);
            res.add( getMonthlyExpiration( dt));
            start.add( Calendar.MONTH, 1);
        }
        return res;
    }

    static public List<String> expiries(String dt, int m) {
        List<String> res = new ArrayList<String>();
        for(int ix = 0; ix < m; ix++)
            res.add(getMonthlyExpiration(dt, ix + 1));
        return res;
    }

    public static void main( String [] args) {
    }
}
