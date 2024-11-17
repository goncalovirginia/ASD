package protocols.agreement.messages;

import io.netty.buffer.ByteBuf;
import org.apache.commons.codec.binary.Hex;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.util.UUID;

/*************************************************
 * This is here just as an example, your solution
 * probably needs to use different message types
 *************************************************/
public class AcceptOKMessage extends ProtoMessage {

    public final static short MSG_ID = 191;

    private final UUID opId;
    private final int instance;
    private final byte[] op;
    private final int missingIndex;

    public AcceptOKMessage(int instance, UUID opId, byte[] op, int index) {
        super(MSG_ID);
        this.instance = instance;
        this.op = op;
        this.opId = opId;
        this.missingIndex = index;
    }

    public AcceptOKMessage(AcceptMessage msg) {
        super(MSG_ID);
        this.instance = msg.getInstance();
        this.op = msg.getOp();
        this.opId = msg.getOpId();
        this.missingIndex = -1;
    }

    public int getInstance() {
        return instance;
    }

    public UUID getOpId() {
        return opId;
    }

    public byte[] getOp() {
        return op;
    }

    public int getMissingIndex() {
        return missingIndex;
    }

    @Override
    public String toString() {
        return "AcceptOKMessage{" +
                "opId=" + opId +
                ", instance=" + instance +
                ", op=" + Hex.encodeHexString(op) +
                '}';
    }

    public static ISerializer<AcceptOKMessage> serializer = new ISerializer<AcceptOKMessage>() {
        @Override
        public void serialize(AcceptOKMessage msg, ByteBuf out) {
            out.writeInt(msg.instance);
            out.writeLong(msg.opId.getMostSignificantBits());
            out.writeLong(msg.opId.getLeastSignificantBits());
            out.writeInt(msg.op.length);
            out.writeBytes(msg.op);
            out.writeInt(msg.missingIndex);
        }

        @Override
        public AcceptOKMessage deserialize(ByteBuf in) {
            int instance = in.readInt();
            long highBytes = in.readLong();
            long lowBytes = in.readLong();
            UUID opId = new UUID(highBytes, lowBytes);
            byte[] op = new byte[in.readInt()];
            in.readBytes(op);
            int missingIndex = in.readInt();
            return new AcceptOKMessage(instance, opId, op, missingIndex);
        }
    };

}
