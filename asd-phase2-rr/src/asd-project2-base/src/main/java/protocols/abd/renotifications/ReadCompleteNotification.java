package protocols.abd.renotifications;

import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;
import java.util.UUID;

public class ReadCompleteNotification extends ProtoNotification {

    public static final short NOTIFICATION_ID = 522;

    private final byte[] key;
    private final int opSeq;
    private final byte[] value;
    private final UUID opId;

    public ReadCompleteNotification(int opSeq, byte[] key, byte[] value, UUID opId) {
        super(NOTIFICATION_ID);
        this.opSeq = opSeq;
        this.key = key;
        this.value = value;
        this.opId = opId;
    }

    public int getOpSeq() {
        return opSeq;
    }

    public byte[] getKey() {
        return key;
    }

    public byte[] getValue() {
        return value;
    }

    public UUID getOpId() {
        return opId;
    }


    @Override
    public String toString() {
        return "WriteCompletedNotification{" +
                "opId=" + opId +
                ", opSeq=" + opSeq +
                ", key=" + key +
                '}';
    }
}
