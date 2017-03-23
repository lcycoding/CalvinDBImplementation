package org.vanilladb.comm.protocols.consensusUtils;

public class TimestampValue implements Comparable<TimestampValue> {
    private long tstamp;
    private PaxosProposal value;
    public TimestampValue(long ts, PaxosProposal v){
        this.tstamp = ts;
        this.value = v;
    }
    
    public long getTstamp(){
        return tstamp;
    }
    
    public PaxosProposal getPaxosProposal(){
        return value;
    }

    public int compareTo(TimestampValue o) {
        if(this.tstamp < o.getTstamp()){
            return -1;
        }
        else if(this.tstamp > o.getTstamp()){
            return 1;
        }
        else{
            return 0;
        }
    }
}
