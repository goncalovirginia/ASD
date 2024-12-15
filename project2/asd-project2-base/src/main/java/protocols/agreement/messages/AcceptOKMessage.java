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
    private final int sequenceNumber;

    public AcceptOKMessage(int instance, int sn, UUID opId, byte[] op) {
        super(MSG_ID);
        this.instance = instance;
        this.sequenceNumber = sn;
        this.op = op;
        this.opId = opId;
    }

    public AcceptOKMessage(AcceptMessage msg) {
        super(MSG_ID);
        this.instance = msg.getInstance();
        this.op = msg.getOp();
        this.opId = msg.getOpId();
        this.sequenceNumber = msg.getSequenceNumber();
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

    public int getSequenceNumber() {
        return sequenceNumber;
    }

    @Override
    public String toString() {
        return "AcceptOKMessage{" +
                "opId=" + opId +
                ", instance=" + instance +
                '}';
    }

    public static ISerializer<AcceptOKMessage> serializer = new ISerializer<AcceptOKMessage>() {
        @Override
        public void serialize(AcceptOKMessage msg, ByteBuf out) {
            out.writeInt(msg.instance);
            out.writeInt(msg.sequenceNumber);
            out.writeLong(msg.opId.getMostSignificantBits());
            out.writeLong(msg.opId.getLeastSignificantBits());
            out.writeInt(msg.op.length);
            out.writeBytes(msg.op);
        }

        @Override
        public AcceptOKMessage deserialize(ByteBuf in) {
            int instance = in.readInt();
            int sn = in.readInt();
            long highBytes = in.readLong();
            long lowBytes = in.readLong();
            UUID opId = new UUID(highBytes, lowBytes);
            byte[] op = new byte[in.readInt()];
            in.readBytes(op);
            
            return new AcceptOKMessage(instance, sn, opId, op);
        }
    };

}
