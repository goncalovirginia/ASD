package protocols.point2point;

import java.io.IOException;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import protocols.dht.replies.LookupReply;
import protocols.dht.requests.Lookup;
import protocols.point2point.requests.Send;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.network.data.Host;

public class EmptyPoint2PointComm extends GenericProtocol {

	private static final Logger logger = LogManager.getLogger(EmptyPoint2PointComm.class);
	
	public final static String PROTOCOL_NAME = "EmptyPoint2PotinComm";
	public final static short PROTOCOL_ID = 400;
	
	private final Host myself;
	private final short DHT_PROTO_ID;
	
	public EmptyPoint2PointComm(Host commHost, short DHT_Proto_ID) {
		super(PROTOCOL_NAME, PROTOCOL_ID);
		this.myself = commHost;
		this.DHT_PROTO_ID = DHT_Proto_ID;
	}
	
	@Override
	public void init(Properties props) throws HandlerRegistrationException, IOException {
		
		//TODO: Must create tcp channel
		
		registerRequestHandler(Send.REQUEST_ID, this::uponSendRequest);
		registerReplyHandler(LookupReply.REPLY_ID, this::uponLookupReply);
	}

	
	private void uponSendRequest(Send request, short protoID) {
		logger.info("Received Send Request: " + request.toString());
		
		Lookup lookup = new Lookup(request.getDestinationPeerID());
		
		sendRequest(lookup, DHT_PROTO_ID);
	}
	
	private void uponLookupReply(LookupReply reply, short protoID) { 
		logger.info("Received Lookup Reply: " + reply.toString());
		
	}
	
}
