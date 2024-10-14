package protocols.dht.kademlia;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.apps.AutomatedApp;
import protocols.dht.chord.requests.LookupRequest;
import protocols.dht.chord.replies.LookupReply;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Properties;

public class KademliaDHT extends GenericProtocol {

	private static final Logger logger = LogManager.getLogger(KademliaDHT.class);

	public final static short PROTOCOL_ID = 500;
	public final static String PROTOCOL_NAME = "KademliaDHT";

	private final Host myself;
	private String myPeerIDHex;
	private byte[] myPeerID;

	public KademliaDHT(Host dhtHost) {
		super(PROTOCOL_NAME, PROTOCOL_ID);
		this.myself = dhtHost;
	}

	@Override
	public void init(Properties props) throws HandlerRegistrationException, IOException {

		//TODO: Must create tcp channel

		this.registerRequestHandler(LookupRequest.REQUEST_ID, this::uponLookup);

		this.myPeerIDHex = props.getProperty(AutomatedApp.PROPERTY_NODE_ID);
		this.myPeerID = new BigInteger(myPeerIDHex, 16).toByteArray();
	}

	private void uponLookup(LookupRequest request, short protoID) {
		logger.info("Received LookupRequest: " + request.toString());

		/* LookupReply lr = new LookupReply(request.getPeerID());

		lr.addElementToPeers(myPeerID, myself);

		sendReply(lr, protoID); */
	}

}
