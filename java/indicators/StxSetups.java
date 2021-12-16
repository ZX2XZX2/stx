package indicators;

import core.StxRecord;

public class StxSetups extends StxRecord {

    public String setups;

    public StxSetups(String dt, String setups) {
        super(dt, 0);
        this.setups = setups;
    }
    public void split(float s) {}
    public void dividend(float d) {}

    public static void main( String[] args) {
    }
}
