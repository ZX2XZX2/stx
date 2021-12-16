package core;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.GregorianCalendar;
import java.util.Properties;
import java.util.TreeMap;

public class StxDB {

    static private Statement s, s1;
    private SimpleDateFormat sdf;
    
    public StxDB( String db_name) throws Exception {
        try {
            s.getConnection();
            s1.getConnection();
        } catch( Exception ex) {
            Class.forName( "org.postgresql.Driver").newInstance();
            String urlStr= "jdbc:postgresql://127.0.0.1:5432/"+ db_name;
            Properties props = new Properties();
            props.setProperty("user", System.getenv("POSTGRES_USER"));
            String postgres_pwd = System.getenv("POSTGRES_PASSWORD");
            if (postgres_pwd != null)
                props.setProperty("password", postgres_pwd);
            s = DriverManager.getConnection(urlStr, props).
                createStatement();
            s1 = DriverManager.getConnection(urlStr, props).
                createStatement();
        }
        sdf= new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss");
    }

    public void close() throws SQLException { s.close();}

    public ResultSet get( String q) throws SQLException {
        return s.executeQuery( q);
    }

    public ResultSet get1(String q) throws SQLException {
        return s1.executeQuery(q);
    }

    public void insert( String i) throws SQLException { s.executeUpdate( i);}

    public void delete( String d) throws SQLException {
        int num_del= s.executeUpdate( d);
        System.err.printf( "Deleted %d records with '%s' ", num_del, d);
    }

    static public PrintWriter uploadData
        ( String db_name, PrintWriter pw, String wfn, String dt, String tbl,
          boolean reorder, boolean last) throws Exception {
        StxDB sdb= new StxDB( db_name);
        PrintWriter res_pw= sdb.uploadData( pw, wfn, dt, tbl, reorder, last);
        sdb.close();
        return res_pw;
    }

    public PrintWriter uploadData( PrintWriter pw, String wfn, String descr,
                                   String tbl, boolean reorder, boolean last)
        throws Exception {
        if( pw!= null) { 
            pw.flush(); pw.close();
            String wfndb= reorder? wfn.replace( ".txt", "_db.txt"): wfn;
            if( reorder) {
                TreeMap<String, TreeMap<String, String>> data= 
                    new TreeMap<String, TreeMap<String, String>>();
                BufferedReader br= new BufferedReader( new FileReader( wfn));
                PrintWriter pwdb= new PrintWriter
                    ( new FileWriter( wfndb, false), false);
                for( String line; ( line= br.readLine())!= null;) {
                    String[] tokens= line.split( "\t");
                    String dt= tokens[ 0].trim(), stk= tokens[ 1].trim();
                    String indx= line.trim();
                    TreeMap<String, String> tm= data.get( dt);
                    if( tm== null) tm= new TreeMap<String, String>();
                    tm.put( stk, indx);
                    data.put( dt, tm);
                }
                for( String d: data.keySet()) {
                    TreeMap<String, String> tm= data.get( d);
                    for( String e: tm.values()) pwdb.write( e+ "\n");
                }
                br.close(); if( pwdb!= null) { pwdb.flush(); pwdb.close();}
            }
            s.executeUpdate( "load data infile '"+ wfndb+ "' into table "+ tbl);
            System.err.println( crtTime()+ ": inserted "+ descr);
        }
        return last? null: new PrintWriter( new FileWriter( wfn, false), false);
    }
    
    private String crtTime() {
        return sdf.format( new GregorianCalendar().getTime());
    }
    
    public static void main( String[] args) throws Exception {
        StxDB sdb= new StxDB( "goldendawn");
        ResultSet rset= sdb.get( "SELECT * FROM `eod` WHERE stk='NFLX'");
        //        ResultSet rset= sdb.get( "SELECT MAX(dt) FROM `options` s WHERE s.und='MSFT'");
        //        String dres= null;
        String dt = null;
        float o = 0, h = 0, l = 0, c = 0, v = 0;
        while( rset.next()) {
            dt = rset.getString(2);
            o = rset.getFloat(3);
            h = rset.getFloat(4);
            l = rset.getFloat(5);
            c = rset.getFloat(6);
            v = rset.getFloat(7);
            StxRec sr = new StxRec(dt, o, h, l, c, v);
            System.err.print(sr.toString());
                
        }
        //            dres= rset.getString( "max(dt)");
        //        if( dres!= null)
        //            System.err.printf( "dres= %s\n", dres);
        // String wfn= StxTS.dbDataDir+ "test.txt";
        // PrintWriter pw= new PrintWriter( new FileWriter( wfn, false), false);
        // pw.write( "AA\t2010-10-10\tsector\tindustry\t1\n");
        // pw.write( "AS\t2010-10-10\tsector\tindustry\t1\n");
        // //pw= sdb.uploadData( pw, wfn, "2010-10-10", "ind_groups", false); 
        // pw.write( "AZ\t2012-12-12\tsector\tindustry\t1\n");
        // pw.write( "AX\t2012-12-12\tsector\tindustry\t1\n");
        // pw= StxDB.uploadData( "goldendawn", pw, wfn, "2012-12-12", "ind_groups", false, true); 
    }
}
