package gui;

import java.awt.Color;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;
import java.awt.event.KeyAdapter;
import java.awt.event.KeyEvent;
import java.awt.event.KeyListener;
import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;

import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JSplitPane;
import javax.swing.JTabbedPane;
import javax.swing.JTextField;

import java.nio.charset.Charset;
import java.nio.file.Files;

import core.StxCal;
import core.StxDB;
import jl.StxxJL;

public class ACtxHD implements KeyListener, ActionListener {
    static JFrame jf; 
    private JTabbedPane jtp_jl;
    private JPanel jpu, jp_trd;
    private JTextField etf, ntf, dtf, dbetf, dbstf, jlf1, jlf2, jlf3, jlp;
    private JButton jb1m, jb3m, jb6m, jb1y, jbjl, jb2y, jb3y, jb5y, jball;
    private JButton rewind, fwd, bak, pick_stk;
    private JButton call, put, c_call, c_put;
    private JButton wl_add, wl_trg, wl_mark, wl_clear, wl_clear_all;
    private JTextField wl_date, wl_spread, wl_days, wl_setups, wl_tag;
    private JComboBox<String> exp, setups_or_trades, chart_scale;
    private JComboBox<Float> strike;
    private JComboBox<Integer> capital;
    private JCheckBox invisible;
    private JLDisplay jld1, jld2, jld3, opts, trades, setups;
    private JLabel jlfl1, jlfl2, jlfl3, trade_status;
    private int resX= 1920, resY= 1080, yr;
    private Chart chrt;
    private StxxJL jl1, jl2, jl3;
    private List<StxTrade> trade_list = new ArrayList<StxTrade>();
    private HashMap<String, Integer> trade_ix = new HashMap<String, Integer>();
    // <cp, expiry, strike> =>
    // <in_date, in_price, in_range, in_opt_px, crt_bid, crt_ask, crt_spot
    String last_scale= "3M", crt_date;
    List<String[]> entries = new ArrayList<String[]>();
    int crt_pos = 0;
    TreeMap<String, Integer> trend_map= new TreeMap<String, Integer>();
    int idx= -1;
    String log_fname, last_opt_date;

    public ACtxHD() {
        
        Calendar c= new GregorianCalendar();
        yr= c.get( Calendar.YEAR);
        last_opt_date = String.format("%d-12-31", (yr - 1));
        String d= String.format( "%d-%02d-%02d", yr,
                                 1+ c.get( Calendar.MONTH),
                                 c.get( Calendar.DAY_OF_MONTH));
        new StxCal( yr+ 2);
        this.crt_date = d;
        if( StxCal.isBusDay( d)== false)
            d= StxCal.prevBusDay( d);
        log_fname = String.format("%s/reports/trades/%s.txt",
                                  System.getenv("HOME"), d);
        jf= new JFrame( "ACTX");
        etf= new JTextField( d); etf.setCaretColor( Color.white);
        etf.setName( "ETF"); etf.addKeyListener( this);
        ntf= new JTextField(); ntf.setCaretColor( Color.white);
        ntf.setName( "NTF"); ntf.addKeyListener( this);
        dtf= new JTextField( "120"); dtf.setCaretColor( Color.white);
        dbetf= new JTextField( "eods"); dbetf.setCaretColor( Color.white);
        dbstf= new JTextField( "dividends"); dbstf.setCaretColor( Color.white);
        jlf1 = new JTextField( "0.5"); jlf1.setCaretColor( Color.white);
        jlf2 = new JTextField( "1.0"); jlf2.setCaretColor( Color.white);
        jlf3 = new JTextField( "2.0"); jlf3.setCaretColor( Color.white);
        jlfl1 = new JLabel("Factor: "+ jlf1.getText());
        jlfl2 = new JLabel("Factor: "+ jlf2.getText());
        jlfl3 = new JLabel("Factor: "+ jlf3.getText());
        jlp= new JTextField( "16"); jlp.setCaretColor( Color.white);
        invisible = new JCheckBox("Invisible");
        invisible.setSelected(false);
        jpu= new JPanel( null);
        jpu.setBackground( Color.black);
        jpu.setForeground( Color.lightGray);
        addC( jpu, ntf, 15, 7, 60, 25);
        addC( jpu, etf, 75, 7, 100, 25);
        etf.setForeground(invisible.isSelected()? Color.black: 
                          Color.lightGray);
        fwd= new JButton( "+"); fwd.addActionListener( this);
        bak= new JButton( "-"); bak.addActionListener( this);
        rewind = new JButton("<");
        rewind.addActionListener(this);
        addC( jpu, rewind, 175, 12, 50, 15);
        addC( jpu, fwd, 225, 12, 50, 15);
        addC( jpu, bak, 275, 12, 50, 15);
        addC( jpu, dtf, 325, 7, 50, 25);
        addC( jpu, invisible, 375, 7, 100, 25);
        pick_stk = new JButton("R");
        pick_stk.addActionListener(this);
        addC( jpu, pick_stk, 500, 7, 50, 25);
        chart_scale = new JComboBox<String>();
        chart_scale.setEditable(false);
        chart_scale.addItem("1M");
        chart_scale.addItem("3M");
        chart_scale.addItem("6M");
        chart_scale.addItem("1Y");
        chart_scale.addItem("JL");
        chart_scale.addItem("2Y");
        chart_scale.addItem("3Y");
        chart_scale.addItem("5Y");
        chart_scale.addItem("All");
        chart_scale.setSelectedIndex(1);
        chart_scale.addActionListener(this);
        addC( jpu, chart_scale,  240, 55, 70, 20);
        jb1m= new JButton( "1M");
        jb1m.setOpaque(true);
        jb1m.addActionListener( this);
        addC( jpu, jb1m, 15, 35, 55, 15);
        jb3m= new JButton( "3M");
        jb3m.addActionListener( this);
        addC( jpu, jb3m, 70, 35, 55, 15);
        jb6m= new JButton( "6M");
        jb6m.addActionListener( this);
        addC( jpu, jb6m, 125, 35, 55, 15);
        jb1y= new JButton( "1Y");
        jb1y.addActionListener( this);
        addC( jpu, jb1y, 180, 35, 55, 15);
        jbjl= new JButton( "JL");
        jbjl.addActionListener( this);
        addC( jpu, jbjl, 235, 35, 55, 15);
        jb2y = new JButton( "2Y");
        jb2y.addActionListener( this);
        addC( jpu, jb2y, 290, 35, 55, 15);
        jb3y= new JButton( "3Y");
        jb3y.addActionListener( this);
        addC( jpu, jb3y, 345, 35, 55, 15);
        jb5y= new JButton( "5Y");
        jb5y.addActionListener( this);
        addC( jpu, jb5y, 400, 35, 55, 15);
        jball= new JButton( "All");
        jball.addActionListener( this);
        addC( jpu, jball, 455, 35, 55, 15);
        addC( jpu, new JLabel("JL: "), 15, 55, 25, 20);
        addC( jpu, jlf1,  40, 55, 50, 20);
        addC( jpu, jlf2,  90, 55, 50, 20);
        addC( jpu, jlf3,  140, 55, 50, 20);
        addC( jpu, jlp,  190, 55, 50, 20);
        
        jp_trd = new JPanel(null);
        jp_trd.setBackground(Color.black);
        jp_trd.setForeground(Color.lightGray);
        call = new JButton("CALL"); call.addActionListener(this);
        put = new JButton("PUT"); put.addActionListener(this);
        c_call = new JButton("CLOSE CALL"); c_call.addActionListener(this);
        c_put = new JButton("CLOSE PUT"); c_put.addActionListener(this);
        strike = new JComboBox<Float>();
        strike.setEditable(true);
        strike.getEditor().getEditorComponent().addKeyListener
            (new KeyAdapter() {
                    @Override
                    public void keyReleased(KeyEvent event) {
                        // System.err.printf("event.getKeyCode() = %d\n",
                        //                 event.getKeyCode());
                        if (event.getKeyCode() == KeyEvent.VK_F1) {
                            etf.requestFocusInWindow();
                        }
                    }
                });     
        exp = new JComboBox<String>();
        exp.setEditable(true);
        exp.getEditor().getEditorComponent().addKeyListener
            (new KeyAdapter() {
                    @Override
                    public void keyReleased(KeyEvent event) {
                        if (event.getKeyCode() == KeyEvent.VK_F1) {
                            etf.requestFocusInWindow();
                        }
                    }
                });     
        capital = new JComboBox<Integer>();
        capital.getEditor().getEditorComponent().addKeyListener
            (new KeyAdapter() {
                    @Override
                    public void keyReleased(KeyEvent event) {
                        if (event.getKeyCode() == KeyEvent.VK_F1) {
                            etf.requestFocusInWindow();
                        }
                    }
                });     
        capital.setEditable(true);
        capital.addItem(125);
        capital.addItem(250);
        capital.addItem(500);
        capital.addItem(750);
        capital.addItem(1000);
        capital.addItem(1500);
        capital.addItem(2000);
        capital.setSelectedIndex(2);
        opts = new JLDisplay(600, 100, 12, invisible.isSelected());
        trades = new JLDisplay(600, 100, 12, invisible.isSelected());
        setups = new JLDisplay(600, 100, 12, invisible.isSelected());
        trade_status = new JLabel("GETTING STARTED . . .");

        wl_add = new JButton("WL ADD"); wl_add.addActionListener(this);
        wl_trg = new JButton("WL TRG"); wl_trg.addActionListener(this);
        wl_mark = new JButton("WL MARK"); wl_mark.addActionListener(this);
        wl_clear = new JButton("WL CLEAR"); wl_clear.addActionListener(this);
        wl_clear_all = new JButton("WL CLEAR ALL"); 
        wl_clear_all.addActionListener(this);
        wl_date = new JTextField(d);
        wl_date.setCaretColor(Color.white);
        wl_date.setName("WLDT"); 
        wl_date.addKeyListener(this);
        wl_spread = new JTextField("12");
        wl_spread.setCaretColor(Color.white);
        wl_spread.setName("WLS"); 
        wl_spread.addKeyListener(this);
        wl_days = new JTextField("5");
        wl_days.setCaretColor(Color.white);
        wl_days.setName("WLD"); 
        wl_days.addKeyListener(this);
        wl_setups = new JTextField("8");
        wl_setups.setCaretColor(Color.white);
        wl_setups.setName("WLSTP"); 
        wl_setups.addKeyListener(this);
        wl_tag = new JTextField("JL");
        wl_tag.setCaretColor(Color.white);
        wl_tag.setName("WLTAG"); 
        wl_tag.addKeyListener(this);
        setups_or_trades = new JComboBox<String>();
        setups_or_trades.setEditable(false);
        setups_or_trades.addItem("JL_Setups");
        setups_or_trades.addItem("Scored_Setups");
        setups_or_trades.addItem("Trades");
        setups_or_trades.addItem("Setups");
        setups_or_trades.setSelectedIndex(0);
        
        addC(jp_trd, call, 5, 5, 80, 15);
        addC(jp_trd, put, 85, 5, 80, 15);
        addC(jp_trd, c_call, 165, 5, 120, 15);
        addC(jp_trd, c_put, 285, 5, 120, 15);
        addC(jp_trd, strike, 405, 0, 80, 20);
        addC(jp_trd, exp, 485, 0, 160, 20);
        addC(jp_trd, capital, 645, 0, 80, 20);
        addC(jp_trd, trade_status, 765, 5, 550, 15);
        addC(jp_trd, opts, 5, 20, 505, 155);
        addC(jp_trd, trades, 510, 20, 630, 155);
        addC(jp_trd, setups, 1140, 20, 140, 190);
        addC(jp_trd, wl_add, 5, 185, 90, 20);
        addC(jp_trd, wl_trg, 95, 185, 90, 20);
        addC(jp_trd, wl_mark, 185, 185, 100, 20);
        addC(jp_trd, wl_clear, 285, 185, 110, 20);
        addC(jp_trd, wl_clear_all, 655, 185, 150, 20);
        addC(jp_trd, wl_date, 395, 185, 100, 25);
        addC(jp_trd, wl_spread, 495, 185, 40, 25);
        addC(jp_trd, wl_days, 535, 185, 40, 25);
        addC(jp_trd, wl_setups, 575, 185, 40, 25);
        addC(jp_trd, wl_tag, 615, 185, 40, 25);
        addC(jp_trd, setups_or_trades, 805, 185, 100, 20);      
        int hd11= 2* resX/ 3;
        addC( jpu, jlfl1, 5, 90, 80, 20);
        jld1= new JLDisplay( hd11 + 40, 220, 12, invisible.isSelected());
        addC( jpu, jld1, 5, 110, resX- hd11, 290);
        addC( jpu, jlfl2,  5, 400, 80, 20);
        jld2= new JLDisplay( hd11- 10, 220, 12, invisible.isSelected());
        addC( jpu, jld2, 5, 420, resX- hd11, 290);
        addC( jpu, jlfl3,  5, 710, 80, 20);
        jld3= new JLDisplay( hd11- 10, 220, 12, invisible.isSelected());
        addC( jpu, jld3, 5, 730, resX- hd11, 290);
        jtp_jl= new JTabbedPane( JTabbedPane.BOTTOM);

        int vert_div = 14 * resY / 19;
        JSplitPane jspv= new JSplitPane( JSplitPane.VERTICAL_SPLIT,
                                         jtp_jl, jp_trd);
        jspv.setOneTouchExpandable(true);
        jspv.setDividerLocation(vert_div);

        jtp_jl.addChangeListener(new ChangeListener() {
                public void stateChanged(ChangeEvent e) {
                    Chart cc = (Chart)jtp_jl.getSelectedComponent();
                    if((cc != null) && (ntf.getText() != cc.stk_name)) {
                        ntf.setText(cc.stk_name);
                        etf.setText(cc.ts.currentDate());
                    }
                }
            });
        
        JSplitPane jspu= new JSplitPane( JSplitPane.HORIZONTAL_SPLIT,
                                         jspv, jpu);
        jspu.setOneTouchExpandable( true);
        jspu.setDividerLocation( hd11);
        jf.pack();
        jf.setSize( resX, resY);
        jf.getContentPane().add( jspu);
        jf.setDefaultCloseOperation( JFrame.EXIT_ON_CLOSE);
        jf.setVisible(true);
    }
    public void keyPressed( KeyEvent e) {
        try {
            //  F1 = 112
            //  F2 = 113
            //  F3 = 114
            //  F4 = 115
            //  F5 = 116
            //  F6 = 117
            //  F7 = 118
            //  F8 = 119
            //  F9 = 120
            // F10 = 121
            // F11 = 122
            // F12 = 123
            int cd= e.getKeyCode(); String src= e.getComponent().getName();
            //          System.err.printf("cd = %d, src = %s\n", cd, src);
            if( src.equals( "ETF")) {
                String ed= etf.getText();
                if (cd == 38) 
                    etf.setText(StxCal.nextBusDay(ed));
                if (cd == 40)
                    etf.setText(StxCal.prevBusDay(ed));
                handleCommonKeys(cd);
            } else if (src.equals("NTF")) {
                handleCommonKeys(cd);
                if(cd == 38) 
                    moveTabRight();
                if(cd == 40) 
                    moveTabLeft();
            } else if (src.equals("WLS") || src.equals("WLD") || 
                       src.equals("WLSTP") || src.equals("WLTAG")) {
                if (cd == KeyEvent.VK_F1)
                    etf.requestFocusInWindow();
                if (cd == KeyEvent.VK_F2)
                    ntf.requestFocusInWindow();
                if (cd == KeyEvent.VK_F3)
                    wl_date.requestFocusInWindow();
            } else if (src.equals("WLDT")) {
                String ed = wl_date.getText();
                if (cd == 38) 
                    wl_date.setText(StxCal.nextBusDay(ed));
                if (cd == 40)
                    wl_date.setText(StxCal.prevBusDay(ed));
                handleCommonKeys(cd);
            }
        } catch (Exception exc) {
            exc.printStackTrace(System.err);
        }
    }

    private void moveTabRight() throws Exception {
        int sel_ix = jtp_jl.getSelectedIndex() + 1;
        if (sel_ix >= jtp_jl.getTabCount())
            sel_ix = 0;
        jtp_jl.setSelectedIndex(sel_ix);
        go();
    }

    private void moveTabLeft() throws Exception {
        int sel_ix = jtp_jl.getSelectedIndex() - 1;
        if(sel_ix < 0)
            sel_ix = jtp_jl.getTabCount() - 1;
        jtp_jl.setSelectedIndex(sel_ix);
        go();
    }

    private void markTab() {
        int ix = jtp_jl.getSelectedIndex();
        jtp_jl.setToolTipTextAt(ix, wl_date.getText());
        jtp_jl.setBackgroundAt(ix, Color.yellow);
    }

    private void clearTab() {
        int ix = jtp_jl.getSelectedIndex();
        jtp_jl.setToolTipTextAt(ix, null);
        jtp_jl.setBackgroundAt(ix, null);
    }

    private void handleCommonKeys(int cd) throws Exception {
        if (cd == KeyEvent.VK_ENTER) 
            go();
        if (cd == 33) {
            increaseScale();
            go();
        }
        if (cd == 34) {
            decreaseScale();
            go();
        }
        if (cd == 35 || cd == 36) {
            move(cd);
            go();
        }
        if (cd == 46) // KeyEvent.VK_GREATER
            moveTabRight();
        if (cd == 44) // KeyEvent.VK_LESS
            moveTabLeft();
        if (cd == KeyEvent.VK_OPEN_BRACKET) 
            markTab();
        if (cd == KeyEvent.VK_CLOSE_BRACKET) 
            clearTab();
        if (cd == KeyEvent.VK_F1)
            openTrade("CALL");
        else if (cd == 113)
            openTrade("PUT");
        else if (cd == 114)
            closeTrade("CLOSE CALL");
        else if (cd == 115)
            closeTrade("CLOSE PUT");
        else if (cd == 116)
            strike.requestFocusInWindow();
        else if (cd == 117)
            exp.requestFocusInWindow();
        else if (cd == 118)
            capital.requestFocusInWindow();
        else if (cd == 123) {
            int sel_ix = jtp_jl.getSelectedIndex();
            jtp_jl.remove(sel_ix);
            if(sel_ix >= jtp_jl.getTabCount())
                sel_ix = jtp_jl.getTabCount() - 1;
            jtp_jl.setSelectedIndex(sel_ix);
        } else if (cd == 120) {
            decreaseScale();
            go();
        } else if (cd == 121) {
            increaseScale();
            go();
        }
    }
    
    private void decreaseScale() {
        switch(last_scale) {
        case "1M":
        case "3M":
            last_scale = "1M";
            break;
        case "6M":
            last_scale = "3M";
            break;
        case "1Y":
            last_scale = "6M";
            break;
        case "JL":
            last_scale = "1Y";
            break;
        case "2Y":
            last_scale = "JL";
            break;
        case "3Y":
            last_scale = "2Y";
            break;
        case "5Y":
            last_scale = "3Y";
            break;
        case "All":
            last_scale = "5Y";
            break;
        }
    }

    private void increaseScale() {
        switch(last_scale) {
        case "1M":
            last_scale = "3M";
            break;
        case "3M":
            last_scale = "6M";
            break;
        case "6M":
            last_scale = "1Y";
            break;
        case "1Y":
            last_scale = "JL";
            break;
        case "JL":
            last_scale = "2Y";
            break;
        case "2Y":
            last_scale = "3Y";
            break;
        case "3Y":
            last_scale = "5Y";
            break;
        case "5Y":
        case "All":
            last_scale = "All";
            break;
        }
    }

    private void move(int cd) {
        int num_bds = 20;
        try {
            num_bds = Integer.parseInt(dtf.getText());
        } catch(Exception ex) {}
        etf.setText(StxCal.moveBusDays(etf.getText(), (cd == 35)?
                                       num_bds: -num_bds));
    }
    // private String getAnalysisDate( String ed, int dir) {
    //     String res= ed;
    //     String[] current_values = analysis_file.get( ed);
    //     if( current_values== null)
    //         return res;
    //     boolean found= false;        
    //     SortedMap<String, String[]> smap= ( dir== 1)? 
    //         analysis_file.tailMap( StxCal.nextBusDay( ed)): 
    //         analysis_file.descendingMap().tailMap( StxCal.prevBusDay( ed));
    //     for( Map.Entry<String, String[]> entry: smap.entrySet()) {
    //         found= true; String[] values= entry.getValue();
    //         for( int ix: idxf) {
    //             if(values[ix].compareTo( current_values[ ix])!= 0) {
    //                 found= false; break;
    //             }
    //         }
    //         if( found== true) { res= entry.getKey(); break;}
    //     }
    //     return res;
    // }
    public void keyReleased(KeyEvent e) {}
    public void keyTyped(KeyEvent e) {}

    public void go() throws Exception {

        String jls, s, e= etf.getText(), tooltip_text = null;
        Color bg_color = null;
        String n= ntf.getText();
        int idx, w= 20, p= Integer.parseInt( jlp.getText());
        float f1= Float.parseFloat( jlf1.getText());
        float f2= Float.parseFloat( jlf2.getText());
        float f3= Float.parseFloat( jlf3.getText());
        etf.setForeground(invisible.isSelected()? Color.black: 
                          Color.lightGray);
        ntf.setForeground(invisible.isSelected()? Color.black: 
                          Color.lightGray);
        jlfl1.setText("Factor: "+ jlf1.getText());
        jlfl2.setText("Factor: "+ jlf2.getText());
        updateSetupPanel();
        if( StxCal.isBusDay( e)== false)
            e= StxCal.nextBusDay( e);
        jls= ( StxCal.year( e)- 2)+ "-01-01";
        s= "1901-01-02";
        idx= jtp_jl.indexOfTab( n);
        if( idx!= -1) {
            tooltip_text = jtp_jl.getToolTipTextAt(idx);
            bg_color = jtp_jl.getBackgroundAt(idx);
            jtp_jl.remove( idx); 
        }
        jl1 = jld1.runJL(n, jls, e, f1, w, p, dbetf.getText(),
                         dbstf.getText());
        // jld1.append( analysis( e));
        jl2 = jld2.runJL(n, jls, e, f2, w, p, dbetf.getText(),
                         dbstf.getText());
        // jld2.append( analysis( e));
        jl3 = jld3.runJL(n, jls, e, f3, w, p, dbetf.getText(),
                         dbstf.getText());
        chrt= new Chart(n, s, e, true, dbetf.getText(), dbstf.getText(), 
                        jl1, jl2, jl3, invisible.isSelected());
        chrt.setScale(last_scale);
        if (idx == -1)
            idx = jtp_jl.getTabCount();
        jtp_jl.insertTab(n, null, chrt, tooltip_text, idx);
        jtp_jl.setBackgroundAt(idx, bg_color);
        jtp_jl.setSelectedIndex(jtp_jl.indexOfTab(n));
        getOptions();
        updateTradeStatus();
        updateSetupStatus();
    }

    private void updateSetupPanel() {
        jld3.clear();
        if(entries.size() == 0)
            return;
        int ix = 0;
        for(String str: entries.get(crt_pos)) {
            switch(ix) {
            case 0: case 1: case 3: case 4: case 9: case 11: case 16: case 21:
                jld3.append(str + '\n');
                break;
            case 5: case 6: case 7: case 8: case 10: case 12: case 13: case 14:
            case 15:
                float val = Float.parseFloat(str);
                jld3.append(String.format("%.2f\n", val));
                break;
            case 17: case 18: case 19: case 20:
                try {
                    float val1 = Float.parseFloat(str);
                    jld3.append(String.format("%.2f\n", val1));
                } catch(Exception ex) {
                    jld3.append("\n");
                }
                break;
            default:
                break;
            }
            ix++;
        }
    }

    public void addC(JPanel p, JComponent c, int x, int y, int h, int w) {
        c.setBackground(Color.black);
        c.setOpaque(true);
        if (c instanceof JButton || c instanceof JComboBox) {
            c.setForeground(Color.black);
            c.setBackground(Color.lightGray);
        } else
            c.setForeground(Color.lightGray);
        p.add(c); c.setBounds(x, y, h, w);
    }

    private void moveDate( int sign) {
        int num= 20;
        try {
            num= Integer.parseInt( dtf.getText());
        } catch (Exception e1) {}
        num*= sign;
        try {
            String crt_date = etf.getText();
            etf.setText( StxCal.moveBusDays( crt_date, num));
            go();
        } catch (Exception e2) {
            e2.printStackTrace( System.err);
        }
    }

    public void actionPerformed(ActionEvent ae) {
        String cmd_name = (String) ae.getActionCommand();
        Chart cc = (Chart) jtp_jl.getSelectedComponent();
        if (cmd_name.equals("1M") || cmd_name.equals("3M") ||
            cmd_name.equals("6M") || cmd_name.equals("1Y") ||
            cmd_name.equals("JL") || cmd_name.equals("2Y") ||
            cmd_name.equals("3Y") || cmd_name.equals("5Y") ||
            cmd_name.equals("All")) {
            cc.setScale( cmd_name);
            last_scale= cmd_name;
        }
        if (ae.getSource() == fwd) moveDate( 1);
        if (ae.getSource() == bak) moveDate( -1);
        if (ae.getSource() == rewind) {
            String rewind_date = cc.rewind();
            etf.setText(rewind_date);
            try {
                go();
            } catch( Exception exc) {
                exc.printStackTrace(System.err);
            }
        }
        if (ae.getSource() == pick_stk) {
            String stk = "NFLX";
            try {
                List<String> lines = Files.readAllLines
                    (new File("../super_liquid_stx.txt").
                     toPath(), Charset.defaultCharset());
                stk = lines.get(ThreadLocalRandom.current().nextInt
                                (0, lines.size()));
            } catch(IOException ioe) {
                ioe.printStackTrace(System.err);
            }
            ntf.setText(stk);
            try {
                go();
            } catch( Exception exc) {
                exc.printStackTrace(System.err);
            }       
        }
        if (ae.getSource() == chart_scale) {
            String new_scale = chart_scale.getSelectedItem().toString();
            cc.setScale(new_scale);
            last_scale= new_scale;
        }
        if (ae.getSource() == wl_add) {
            String table_name = setups_or_trades.getSelectedItem().toString().
                toLowerCase();
            List<String> stx = null;
            if (table_name.equals("jl_setups"))
                stx = getJLSetupStocks();
            else if (table_name.equals("scored_setups"))
                stx = getScoredSetupStocks();
            else if (table_name.equals("setups"))
                stx = getSetupStocks();
            else
                stx = getTradeStocks();
            for(String stk: stx) {
                ntf.setText(stk);
                etf.setText(wl_date.getText());
                try {
                    go();
                } catch( Exception exc) {
                    exc.printStackTrace(System.err);
                }
            }
        }
        if (ae.getSource() == wl_trg) {
            String table_name = setups_or_trades.getSelectedItem().toString().
                toLowerCase();
            wl_date.setText(StxCal.nextBusDay(wl_date.getText()));
            if (table_name.equals("setups")) {
                List<String> stx = getSetupStocks(true);
                for (int ix = jtp_jl.getTabCount() - 1; ix >= 0; ix--) {
                    String stk = jtp_jl.getTitleAt(ix);
                    if (!stx.contains(stk) && 
                        (jtp_jl.getToolTipTextAt(ix) == null))
                        jtp_jl.remove(ix);
                    else {
                        ntf.setText(stk);
                        etf.setText(wl_date.getText());
                        try {
                            go();
                        } catch( Exception exc) {
                            exc.printStackTrace(System.err);
                        }
                    }
                }
            } else {
                for (int ix = jtp_jl.getTabCount() - 1; ix >= 0; ix--) {
                    String stk = jtp_jl.getTitleAt(ix);
                    ntf.setText(stk);
                    etf.setText(wl_date.getText());
                    try {
                        go();
                    } catch( Exception exc) {
                        exc.printStackTrace(System.err);
                    }
                }
            }
        }
        if (ae.getSource() == wl_mark)
            markTab();
        if (ae.getSource() == wl_clear) 
            clearTab();
        if (ae.getSource() == wl_clear_all) {
            for (int ix = jtp_jl.getTabCount() - 1; ix >= 0; ix--) {
                if (jtp_jl.getToolTipTextAt(ix) == null)
                    jtp_jl.remove(ix);
            }
        }
        if(cmd_name.equals("CALL") || cmd_name.equals("PUT"))
            openTrade(cmd_name);
        if(cmd_name.equals("CLOSE CALL") || cmd_name.equals("CLOSE PUT"))
            closeTrade(cmd_name);
    }

    private List<String> getTradeStocks() {
        String dt = wl_date.getText();
        List<String> res = new ArrayList<String>();
        StringBuilder q = new StringBuilder("SELECT DISTINCT stk FROM ");
        StringBuilder stk_list = new StringBuilder(dt);
        stk_list.append(":");
        q.append("trades WHERE in_dt='").append(dt).append("' AND tag='").
            append(wl_tag.getText()).append("'");
        //         System.err.println("getTradeStocks: q = " + q.toString());
        try {
            StxDB sdb = new StxDB(System.getenv("POSTGRES_DB"));
            ResultSet sret = sdb.get1(q.toString());
            while(sret.next()) {
                String stx = sret.getString(1);
                res.add(stx);
                stk_list.append(" ").append(stx);
            }
            System.err.printf("%s\n", stk_list.toString());
        } catch( Exception ex) {
            System.err.println("Failed to get trades: ");
            ex.printStackTrace(System.err);
        }
        return res;
    }    
    
    private List<String> getSetupStocks() {
        return getSetupStocks(false);
    }

    private List<String> getJLSetupStocks() {
        String dt = wl_date.getText();
        String exp = StxCal.getMonthlyExpiration(StxCal.nextBusDay(dt));
        int num_stks = Integer.parseInt(wl_setups.getText());
        List<String> res = new ArrayList<String>();

        StringBuilder q =
            new StringBuilder("SELECT distinct stk FROM jl_setups ");
        q.append("WHERE stk IN (SELECT stk FROM leaders WHERE expiry='").
            append(exp).append("' AND opt_spread <= ").
            append(wl_spread.getText()).append(") AND dt='").append(dt).
            append("' AND setup in ('JL_P', 'JL_B', 'JL_S')");
        System.err.println("getJLSetupStocks: q = " + q.toString());
        try {
            StxDB sdb = new StxDB(System.getenv("POSTGRES_DB"));
            ResultSet sret = sdb.get1(q.toString());
            while(sret.next())
                res.add(sret.getString(1));
        } catch( Exception ex) {
            System.err.println("Failed to get scored setups: ");
            ex.printStackTrace(System.err);
        }
        return res;
    }

    private List<String> getScoredSetupStocks() {
        String dt = wl_date.getText();
        String exp = StxCal.getMonthlyExpiration(StxCal.nextBusDay(dt));
        int num_stks = Integer.parseInt(wl_setups.getText());
        List<String> res = new ArrayList<String>();
        StringBuilder q = new StringBuilder("SELECT stk FROM setup_scores ");
        q.append("WHERE stk IN (SELECT stk FROM leaders WHERE expiry='").
            append(exp).append("' AND opt_spread <= ").
            append(wl_spread.getText()).append(") AND dt='").append(dt).
            append("' AND trigger_score != 0 ORDER BY ABS(trigger_score+").
            append("trend_score) DESC LIMIT ").append(num_stks);
        System.err.println("getScoredSetupStocks: q = " + q.toString());
        try {
            StxDB sdb = new StxDB(System.getenv("POSTGRES_DB"));
            ResultSet sret = sdb.get1(q.toString());
            while(sret.next())
                res.add(sret.getString(1));
        } catch( Exception ex) {
            System.err.println("Failed to get scored setups: ");
            ex.printStackTrace(System.err);
        }
        return res;
    }

    private List<String> getSetupStocks(boolean triggered_only) {
        String crt_dt = wl_date.getText();
        String dt = (!triggered_only) ? StxCal.nextBusDay(crt_dt): crt_dt;
        int spread = Integer.parseInt(wl_spread.getText());
        int days = Integer.parseInt(wl_days.getText());
        int setups = Integer.parseInt(wl_setups.getText()), stk_setups = 0;
        List<String> res = new ArrayList<String>();
        HashMap<String, Integer> dct = new HashMap<String, Integer>();
        String sdt = StxCal.moveBusDays(crt_dt, -days);
        StringBuilder q = new StringBuilder("SELECT DISTINCT stk FROM ");
        q.append("setups WHERE dt='").append(dt).append("' AND ").
            append("setup IN ('JC_1234', 'JC_5DAYS')");
        if (triggered_only)
            q.append(" AND triggered=true");
        System.err.println("getSetupStocks: q = " + q.toString());
        String expiry = StxCal.getMonthlyExpiration(dt);
        try {
            StxDB sdb = new StxDB(System.getenv("POSTGRES_DB"));
            ResultSet sret = sdb.get1(q.toString());
            while(sret.next()) {
                String stk = sret.getString(1);
                StringBuilder q1 = new StringBuilder();
                q1.append("SELECT opt_spread from leaders where expiry='").
                    append(expiry).append("' AND stk='").append(stk).
                    append("'");
                ResultSet rset1 = sdb.get(q1.toString());
                while (rset1.next())
                    dct.put(stk, rset1.getInt(1));
            }
            for (Map.Entry<String, Integer> entry: dct.entrySet()) {
                String stk = entry.getKey();
                if (entry.getValue() <= spread) {
                    StringBuilder q2 = new StringBuilder();
                    q2.append("SELECT count(*) FROM setups WHERE dt BETWEEN").
                        append(" '").append(sdt).append("' AND '").
                        append(crt_dt).append("' AND stk='").append(stk).
                        append("' AND setup IN ('GAP_HV', 'STRONG_CLOSE')");
                    ResultSet rset2 = sdb.get(q2.toString());
                    stk_setups = 0;
                    while (rset2.next())
                        stk_setups = rset2.getInt(1);
                    if (stk_setups >= setups)
                        res.add(stk);
                }
            }
        } catch( Exception ex) {
            System.err.println("Failed to get setups: ");
            ex.printStackTrace(System.err);
        }
        return res;
    }

    private void openTrade(String cmd_name) {
        String dt = etf.getText();
        String expiry =
            StxCal.getMonthlyExpiration(dt, exp.getSelectedIndex() + 1);
        StxTrade trd = new StxTrade(ntf.getText(), cmd_name, expiry, dt,
                                    (Float)strike.getSelectedItem(),
                                    chrt.getSR(dt).c, jl1.avgRg(),
                                    Float.parseFloat(capital.
                                                     getSelectedItem().
                                                     toString()), 
                                    false, crt_date);
        trade_ix.put(trd.key(), trade_list.size());
        trade_list.add(trd);
        updateTradeStatus();
        String trade_key = String.format
            ("%s_%s_%d_%.2f", ntf.getText(), cmd_name,
             StxCal.numBusDaysExpiry(dt, expiry), strike.getSelectedItem());
        trade_status.setText(String.format("OPENED %s", trade_key));
        markTab();
    }

    private void closeTrade(String cmd_name) {
        String cp = cmd_name.equals("CLOSE CALL")? "c": "p";
        String dt = etf.getText();
        String expiry =
            StxCal.getMonthlyExpiration(dt, exp.getSelectedIndex() + 1);
        String trade_key = String.format("%s_%s_%s_%.2f", ntf.getText(), cp,
                                         expiry, strike.getSelectedItem());
        int ix = trade_ix.get(trade_key);
        StxTrade trd = trade_list.get(ix);
        trd.close(log_fname);
        updateTradeStatus();
        String trd_key = String.format("%s_%s_%d_%.2f", ntf.getText(), cp,
                                       StxCal.numBusDaysExpiry(dt, expiry),
                                       strike.getSelectedItem());
        trade_status.setText(String.format("CLOSED %s", trd_key));
        clearTab();
    }

    private void updateTradeStatus() {
        String dt = etf.getText();
        float cc = chrt.getSR(dt).c;
        trades.clear();
        for(StxTrade trd: trade_list) {
            trd.update(dt, cc, log_fname);
            trades.append(trd.ui_entry());
        }
    }

    private void updateSetupStatus() {
        String edt = etf.getText(), sdt, stk = ntf.getText();
        int days = 5;
        try {
            days = Integer.parseInt(wl_days.getText());
        } catch (Exception ex) {
            System.err.println("Failed to get num days for setups:");
            ex.printStackTrace(System.err);
        }
        sdt = StxCal.moveBusDays(edt, -days);
        setups.clear();
        if (setups_or_trades.getSelectedItem().equals("Setups")) {
            StringBuilder q = new StringBuilder("SELECT * FROM setups WHERE ");
            q.append("stk='").append(stk).append("' AND dt BETWEEN '").
                append(sdt).append("' AND '").append(edt).
                append("' AND setup ").
                append("IN ('GAP_HV', 'STRONG_CLOSE') ORDER BY dt");
            try {
                StxDB sdb = new StxDB(System.getenv("POSTGRES_DB"));
                ResultSet rset = sdb.get1(q.toString());
                while(rset.next()) {
                    int num_days = StxCal.numBusDays(rset.getString(1), edt);
                    String display_setup = rset.getString(3).equals("GAP_HV")?
                        "GAP": "SC";
                    String stp = String.format("D_%d %s\n", num_days, 
                                               display_setup);
                    String dir = rset.getString(4);
                    setups.append(stp, dir.equals("U")? Color.green: Color.red);
                }
            } catch( Exception ex) {
                System.err.println("Failed to get setups: ");
                ex.printStackTrace(System.err);
            }
        } else {
            StringBuilder q1 = new StringBuilder("SELECT * FROM setup_scores ");
            q1.append("WHERE stk='").append(stk).append("' AND dt='").
                append(edt).append("'");
            StringBuilder q2 = new StringBuilder("SELECT * FROM jl_setups ");
            q2.append("WHERE stk='").append(stk).append("' AND dt ").
                append("BETWEEN '").append(sdt).append("' AND '").
                append(edt).append("' ORDER BY dt");
            StringBuilder q3 = new StringBuilder();
            q3.append("SELECT indicators->>'rs_rank' FROM indicators ").
                append("WHERE stk='").append(stk).append("' AND dt ='").
                append(edt).append("'");
            try {
                StxDB sdb = new StxDB(System.getenv("POSTGRES_DB"));
                ResultSet rset_rs = sdb.get1(q3.toString());
                int rs = 0;
                while(rset_rs.next())
                    rs = rset_rs.getInt(1);
                ResultSet rset = sdb.get1(q1.toString());
                String stk_summary = "";
                while(rset.next())
                    stk_summary = String.format("%s %4d %4d\n",
                                                rset.getString(2),
                                                rs,
                                                rset.getInt(4));
                ResultSet sret = sdb.get(q2.toString());
                while(sret.next()) {
                    int num_days = StxCal.numBusDays(sret.getString(1), edt);
                    String setup_name = sret.getString(3).toUpperCase();
                    String stp_name = (setup_name.length() > 4)?
                        setup_name.substring(0, 4): setup_name;
                    String stp = String.format("D_%d %4s %4d\n", num_days, 
                                               stp_name, sret.getInt(7));
                    setups.append(stp, 
                                  sret.getString(5).equals("U")? Color.green:
                                  Color.red);
                }
                setups.append(stk_summary, Color.white);
            } catch( Exception ex) {
                System.err.println("Failed to get setups: ");
                ex.printStackTrace(System.err);
            }
        }
    }
    
    private void getOptions() {
        String stk = ntf.getText(), und = stk, ed = etf.getText();
        if(und == null || und.equals("") || ed == null || ed.equals(""))
            return;
        String [] tokens = stk.split("[.]");
        if (tokens.length == 2) {
            try {
                Integer.parseInt(tokens[1]);
                und = tokens[0];
            } catch (Exception ex) {}
        }
        List<String> expiries = StxCal.expiries(ed, 2);
        List<Float> strikes = new ArrayList<Float>();
        float min_dist = 10000;
        int atm_ix = -1, strike_ix = -1;
        float cc = chrt.getSR(ed).c;
        StringBuilder q1= new StringBuilder("SELECT DISTINCT strike FROM ");
        q1.append("options WHERE und='").append(und).append("' AND ").
            append("dt='").append(ed).append("' AND expiry='").
            append(expiries.get(1)).append("' ").append("ORDER BY strike");
        // System.err.println("q1 = " + q1.toString());
        try {
            StxDB sdb = new StxDB(System.getenv("POSTGRES_DB"));
            ResultSet rset = sdb.get(q1.toString());
            while(rset.next()) {
                strike_ix++;
                float s = (float)(rset.getInt(1) / 100.0);
                float dist = Math.abs(s - cc);
                if(dist <= min_dist) {
                    min_dist = dist;
                    atm_ix = strike_ix;
                }
                strikes.add(s);
            }
            if(strikes.size() == 0) {
                String q1_1 = q1.toString().replace(expiries.get(1),
                                                    expiries.get(0));
                ResultSet rset1 = sdb.get(q1_1);
                while(rset1.next()) {
                    strike_ix++;
                    float s = (float)(rset1.getInt(1) / 100.0);
                    float dist = Math.abs(s - cc);
                    if(dist <= min_dist) {
                        min_dist = dist;
                        atm_ix = strike_ix;
                    }
                    strikes.add(s);
                }
            }
        } catch( Exception ex) {
            System.err.println("Failed to get strikes for " + und + ":");
            ex.printStackTrace(System.err);
        }
        opts.clear();
        if(invisible.isSelected())
            opts.append(String.format
                        ("%10d                            %10d\n",
                         StxCal.numBusDaysExpiry(ed, expiries.get(0)),
                         StxCal.numBusDaysExpiry(ed, expiries.get(1))));
        else
            opts.append(String.format("%s                            %s\n",
                                      expiries.get(0), expiries.get(1)));
        List<Float> sel_strikes = new ArrayList<Float>();
        int len = strikes.size();
        if(atm_ix > 1) sel_strikes.add(strikes.get(atm_ix - 2));
        if(atm_ix > 0) sel_strikes.add(strikes.get(atm_ix - 1));
        if(atm_ix >= 0) sel_strikes.add(strikes.get(atm_ix));
        if(atm_ix < len - 1) sel_strikes.add(strikes.get(atm_ix + 1));
        if(atm_ix < len - 2) sel_strikes.add(strikes.get(atm_ix + 2));
        if(sel_strikes.size() == 0)
            return;
        HashMap<Float, HashMap<String, HashMap<String, String>>> opt_dct =
            new HashMap<Float, HashMap<String, HashMap<String, String>>>();
        for(float s: sel_strikes) {
            HashMap<String, HashMap<String, String>> strike_dct =
                new HashMap<String, HashMap<String, String>>();
            for(String expiry: expiries) {
                HashMap<String, String> exp_dct = new HashMap<String, String>();
                exp_dct.put("c", "            ");
                exp_dct.put("p", "            ");
                strike_dct.put(expiry, exp_dct);
            }
            opt_dct.put(s, strike_dct);
        }
        StringBuffer s_sb = new StringBuffer("(");
        strike_ix = 0;
        for(float s: sel_strikes) {
            if(strike_ix > 0) s_sb.append(",");
            s_sb.append((int)(s * 100));
            ++strike_ix;
        }
        s_sb.append(")");
        StringBuilder q2= new StringBuilder("SELECT * FROM options");
        q2.append(" WHERE und='").append(und).append( "' AND dt='").
            append(ed).append("' and expiry in ('").
            append(expiries.get(0)).append("', '").
            append(expiries.get(1)).append("') and strike in ").
            append(s_sb.toString()).append(" order by expiry, strike, cp");
        // System.err.println(q2.toString());
        try {
            StxDB sdb = new StxDB(System.getenv("POSTGRES_DB"));
            ResultSet rset = sdb.get(q2.toString());
            while(rset.next()) {
                String expiry = rset.getString(1), cp = rset.getString(3);
                float s = (float)(rset.getInt(4) / 100.0);
                float bid = (float)(rset.getInt(6) / 100.0);
                float ask = (float)(rset.getInt(7) / 100.0);
                opt_dct.get(s).get(expiry).
                    put(cp, String.format("%5.2f/%5.2f ", bid, ask));
                // System.err.printf("expiry = %s, cp = %s, s = %.2f, bid = %.2f, ask = %.2f\n", expiry, cp, s, bid, ask);
            }
        } catch( Exception ex) {
            System.err.println("Failed to get options for " + und + ":");
            ex.printStackTrace(System.err);
        }
        exp.removeAllItems();
        strike.removeAllItems();
        for(float s: sel_strikes) {
            StringBuffer s_row = new StringBuffer();
            s_row.append("C:").
                append(opt_dct.get(s).get(expiries.get(0)).get("c")).
                append("P:").
                append(opt_dct.get(s).get(expiries.get(0)).get("p")).
                append(String.format(" | %6.2f | ", s)).
                append("C:").
                append(opt_dct.get(s).get(expiries.get(1)).get("c")).
                append(" P:").
                append(opt_dct.get(s).get(expiries.get(1)).get("p")).
                append("\n");
            opts.append(s_row.toString());
            strike.addItem(s);
        }
        strike.setSelectedItem(strikes.get(atm_ix));

        for(String expiry: expiries)
            if(invisible.isSelected())
                exp.addItem(String.format("%d",
                                          StxCal.numBusDaysExpiry(ed, 
                                                                  expiry)));
            else
                exp.addItem(String.format("%d (%s)",
                                          StxCal.numBusDaysExpiry(ed, expiry),
                                          expiry));
        exp.setSelectedIndex(1);
    }
    
    public static void main( String[] args) {
        try {
            new ACtxHD();
        } catch( Exception e) {
            e.printStackTrace( System.err);
        }
    }
    // TODO: load 4 JL calculations, re-dimension the right panel
}
