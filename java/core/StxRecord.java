package core;

public class StxRecord {

    public String date;
    public float ev= 0;
    public char en= '\0';
    public boolean hb4l= false, ef= false;

    public StxRecord() {}
    public StxRecord( String date, float v) { this.date= date; hb4l= false;}
    public StxRecord( String s) {}
    public StxRecord( StxRecord sr) {
        date= sr.date; ev= sr.ev; en= sr.en; ef= sr.ef; hb4l= sr.hb4l;
    }
    public void split( float s) {}
    public void dividend( float d) {}
    public String toString() { return "";}
}
