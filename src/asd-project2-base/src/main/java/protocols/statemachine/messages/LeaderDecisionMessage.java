package protocols.statemachine.messages;

import io.netty.buffer.ByteBuf;
import org.apache.commons.codec.binary.Hex;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import java.util.UUID;

public class LeaderDecisionMessage extends ProtoMessage {

    public final static short MSG_ID = 127;

    private final UUID opId;
    private final int instance;
    private final byte[] op;

    public LeaderDecisionMessage(int instance, UUID opId, byte[] op) {
        super(MSG_ID);
        this.instance = instance;
        this.opId = opId;
        this.op = op;
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

    @Override
    public String toString() {
        return "LeaderDecisionMessage{" +
                "opId=" + opId +
                ", instance=" + instance +
                ", op=" + Hex.encodeHexString(op) +
                '}';
    }

    public static ISerializer<LeaderDecisionMessage> serializer = new ISerializer<LeaderDecisionMessage>() {
        @Override
        public void serialize(LeaderDecisionMessage msg, ByteBuf out) {
            out.writeInt(msg.instance);
            out.writeLong(msg.opId.getMostSignificantBits());
            out.writeLong(msg.opId.getLeastSignificantBits());
            out.writeInt(msg.op.length);
            out.writeBytes(msg.op);
        }

        @Override
        public LeaderDecisionMessage deserialize(ByteBuf in) {
            int instance = in.readInt();
            long highBytes = in.readLong();
            long lowBytes = in.readLong();
            UUID opId = new UUID(highBytes, lowBytes);
            byte[] op = new byte[in.readInt()];
            in.readBytes(op);
            return new LeaderDecisionMessage(instance, opId, op);
        }
    };

}
