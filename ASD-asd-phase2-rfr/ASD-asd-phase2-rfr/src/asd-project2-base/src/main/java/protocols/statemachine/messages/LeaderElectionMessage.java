package protocols.statemachine.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

/*************************************************
 * This is here just as an example, your solution
 * probably needs to use different message types
 *************************************************/
public class LeaderElectionMessage extends ProtoMessage {

    public final static short MSG_ID = 123;

    public LeaderElectionMessage() {
        super(MSG_ID);
    }

    @Override
    public String toString() {
        return "LeaderElectionMessage{}";
    }

    public static ISerializer<LeaderElectionMessage> serializer = new ISerializer<LeaderElectionMessage>() {
        @Override
        public void serialize(LeaderElectionMessage msg, ByteBuf out) {
        }

        @Override
        public LeaderElectionMessage deserialize(ByteBuf in) {
            return new LeaderElectionMessage();
        }
    };

}
