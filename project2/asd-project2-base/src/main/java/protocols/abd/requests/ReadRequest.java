package protocols.abd.requests;

import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;
import org.apache.commons.codec.binary.Hex;

import java.util.UUID;

public class ReadRequest extends ProtoRequest {

    public static final short REQUEST_ID = 510;

    private final UUID opId;
    private final byte[] key;

    public ReadRequest(UUID opId, byte[] key) {
        super(REQUEST_ID);
        this.opId = opId;
        this.key = key;
    }

    public byte[] getKey() {
        return key;
    }

    public UUID getOpId() {
        return opId;
    }

    @Override
    public String toString() {
        return "ReadRequest{" +
                "opId=" + opId +
                ", key=" + Hex.encodeHexString(key) +
                '}';
    }
}
