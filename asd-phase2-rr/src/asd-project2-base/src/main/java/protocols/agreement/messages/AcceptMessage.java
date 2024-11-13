package protocols.agreement.messages;

import io.netty.buffer.ByteBuf;
import org.apache.commons.codec.binary.Hex;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.UUID;

/*************************************************
 * This is here just as an example, your solution
 * probably needs to use different message types
 *************************************************/
public class AcceptMessage extends ProtoMessage {

    public final static short MSG_ID = 198;

    private final UUID opId;
    private final int instance;
    private final byte[] op;
    private final Host newReplica;
    private final boolean changingMembership;
    private final boolean adding;
    private final int lastChosen;

    public AcceptMessage(int instance, UUID opId, byte[] op, int last) {
        super(MSG_ID);
        this.instance = instance;
        this.op = op;
        this.opId = opId;
        this.changingMembership = false;
        this.newReplica = null;
        this.adding = false;
        this.lastChosen = last;
    }

    public AcceptMessage(int instance, Host host, boolean adding) {
        super(MSG_ID);
        this.instance = instance;
        this.op = null;
        this.opId = null;
        this.changingMembership = true;
        this.newReplica = host;
        this.adding = adding;
        this.lastChosen = 0;
    }
    
    public int getInstance() {
        return instance;
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

    public Host getNewReplica() {
        return newReplica;
    }

    public boolean isAddOrRemoving() {
        return changingMembership;
    }

    public boolean isAdding() {
        return adding;
    }

    public boolean isRemoving() {
        return (!adding && changingMembership);
    }

    @Override
    public String toString() {
        if (!changingMembership) {
            return "AcceptMessage{" +
                    "opId=" + opId +
                    ", instance=" + instance +
                    ", op=" + Hex.encodeHexString(op) +
                    '}';
        } else {
            return "AcceptAddRemoveMessage{" +
                    ", instance=" + instance +
                    ", newReplica=" + newReplica +
                    '}';
        }
    }

    public static ISerializer<AcceptMessage> serializer = new ISerializer<AcceptMessage>() {
        @Override
        public void serialize(AcceptMessage msg, ByteBuf out) throws IOException {
            out.writeInt(msg.instance);
            out.writeBoolean(msg.changingMembership);

            if (msg.changingMembership) {
                Host.serializer.serialize(msg.newReplica, out);
                out.writeBoolean(msg.adding);
            } else {
                out.writeLong(msg.opId.getMostSignificantBits());
                out.writeLong(msg.opId.getLeastSignificantBits());
                out.writeInt(msg.op.length);
                out.writeBytes(msg.op);
                out.writeInt(msg.lastChosen);
            }   
        }

        @Override
        public AcceptMessage deserialize(ByteBuf in) throws IOException {
            int instance = in.readInt();
            boolean isReplicaOp = in.readBoolean();

            if (isReplicaOp) {
                Host nReplica = Host.serializer.deserialize(in);
                boolean add = in.readBoolean();
                return new AcceptMessage(instance, nReplica, add);    
            } else {
                long highBytes = in.readLong();
                long lowBytes = in.readLong();
                UUID opId = new UUID(highBytes, lowBytes);
                byte[] op = new byte[in.readInt()];
                in.readBytes(op);
                int last = in.readInt();
                return new AcceptMessage(instance, opId, op, last);
            } 
        }
    };

}
