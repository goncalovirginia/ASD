package protocols.agreement.messages;

import io.netty.buffer.ByteBuf;
import org.apache.commons.codec.binary.Hex;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.UUID;

public class AcceptMessage extends ProtoMessage {

    public final static short MSG_ID = 198;

    private final UUID opId;
    private final int instance;
    private final byte[] op;
    private final int sequenceNumber;

    private final int lastChosen;

    public AcceptMessage(int instance, int sn, UUID opId, byte[] op, int last) {
        super(MSG_ID);
        this.instance = instance;
        this.op = op;
        this.opId = opId;
        this.sequenceNumber = sn;
        this.lastChosen = last;
    }
    
    public int getInstance() {
        return instance;
    }

    public int getSeqNumber() {
        return sequenceNumber;
    }

    public int getLastChosen() {
        return lastChosen;
    }

    public UUID getOpId() {
        return opId;
    }

    public byte[] getOp() {
        return op;
    }



    @Override
    public String toString() {
        return "AcceptAddRemoveMessage{" +
                ", instance=" + instance +
                ", sequenceNumber=" + sequenceNumber +
                '}';
        
    }

    public static ISerializer<AcceptMessage> serializer = new ISerializer<AcceptMessage>() {
        @Override
        public void serialize(AcceptMessage msg, ByteBuf out) throws IOException {
            out.writeInt(msg.instance);
            out.writeInt(msg.sequenceNumber);

            out.writeLong(msg.opId.getMostSignificantBits());
            out.writeLong(msg.opId.getLeastSignificantBits());
            out.writeInt(msg.op.length);
            out.writeBytes(msg.op);
            out.writeInt(msg.lastChosen);
            
        }

        @Override
        public AcceptMessage deserialize(ByteBuf in) throws IOException {
            int instance = in.readInt();
            int sn = in.readInt();

            long highBytes = in.readLong();
            long lowBytes = in.readLong();
            UUID opId = new UUID(highBytes, lowBytes);
            byte[] op = new byte[in.readInt()];
            in.readBytes(op);
            int last = in.readInt();
            return new AcceptMessage(instance, sn, opId, op, last);
             
        }
    };

}
