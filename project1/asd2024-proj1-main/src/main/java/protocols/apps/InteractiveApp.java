package protocols.apps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.point2point.notifications.Deliver;
import protocols.point2point.requests.Send;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import utils.HashProducer;

import java.util.*;

public class InteractiveApp extends GenericProtocol {
	private static final Logger logger = LogManager.getLogger(InteractiveApp.class);

	//Protocol information, to register in babel
	public static final String PROTO_NAME = "AutomateCommunicationApp";
	public static final short PROTO_ID = 300;

	private final short commProtoId;

	//Number of different peers to be considered;
	private final int nPeers;

	//Number of bits used for peer ID's
	private final int idBits;

	//random seed for topic generation
	private final int randomSeed;

	private final int processSequenceNumber;


	private final ArrayList<String> peerIDsHex;
	private final ArrayList<byte[]> peerIDs;
	private final byte[] myPeerID;
	private final String myPeerIDHex;

	private Random r;

	public static final String PROPERTY_NODE_ID = "node_opaque_id";

	public InteractiveApp(Properties properties, short commProtoId) throws HandlerRegistrationException {
		super(PROTO_NAME, PROTO_ID);
		this.commProtoId = commProtoId;

		//Read configurations
		this.nPeers = Integer.parseInt(properties.getProperty("n_peers"));
		this.idBits = Integer.parseInt(properties.getProperty("id_bits"));
		this.randomSeed = Integer.parseInt(properties.getProperty("random_seed", "10000"));

		this.peerIDsHex = new ArrayList<String>(nPeers);
		this.peerIDs = new ArrayList<byte[]>(nPeers);

		//Generate Topics
		r = new Random(this.randomSeed);
		for (int i = 0; i < this.nPeers; i++) {
			byte[] value = new byte[100];
			r.nextBytes(value);
			byte[] id = HashProducer.hashValue("Node" + i + Arrays.toString(value), idBits);
			this.peerIDs.add(id);
			this.peerIDsHex.add(HashProducer.toNumberFormat(id).toString(16));
		}

		if (properties.containsKey("processSequence")) {
			this.processSequenceNumber = Integer.parseInt(properties.getProperty("processSequence"));
			this.myPeerID = this.peerIDs.get(this.processSequenceNumber - 1);
			this.myPeerIDHex = this.peerIDsHex.get(this.processSequenceNumber - 1);
			properties.put(PROPERTY_NODE_ID, this.myPeerIDHex);
		} else {
			this.processSequenceNumber = -1;
			this.myPeerID = null;
			this.myPeerIDHex = null;
			System.err.println("You must indicate the sequence number of the process [1,npeers] using the property 'processSequence'");
			System.exit(1);
		}

		r = new Random(HashProducer.randomInitializer(myPeerID));

		//Setup handlers
		subscribeNotification(Deliver.NOTIFICATION_ID, this::uponDeliver);

	}

	@Override
	public void init(Properties props) {
		//Wait prepareTime seconds before starting
		logger.info("Waiting...");

		Thread interactiveThread = new Thread(new Runnable() {

			@Override
			public void run() {
				String line;
				String[] components;
				Scanner sc = new Scanner(System.in);
				while (true) {
					System.out.print("> ");
					System.out.flush();
					line = sc.nextLine();
					components = line.split(" ");
					switch (components[0]) {
						case "send":
							if (components.length < 2 || components.length > 3) {
								logger.error("Usage: send <indexofnode> [msg-with-no-spaces]");
							} else {
								Send send = new Send(myPeerID, peerIDs.get(Integer.parseInt(components[1])), UUID.randomUUID(), components[2].getBytes());
								logger.info("Sending: {} -> {} : {} ({})", myPeerIDHex, send.getDestinationPeerIDHex(), send.getMessageID().toString(), send.getMessagePayload().length);
								//And send it to the dissemination protocol
								sendRequest(send, commProtoId);
							}
							break;
						case "exit":
							if (components.length != 1) {
								logger.error("Usage: exit");
							} else {
								sc.close();
								System.exit(0);
							}
							break;
						case "help":
						default:
							logger.error("Commands:");
							logger.error("send [idexofnode] <msg-with-no-spaces>");
							logger.error("exit");
							break;
					}
				}
			}
		});
		interactiveThread.start();
	}


	private void uponDeliver(Deliver message, short sourceProto) {
		//Upon receiving a message, simply print it
		logger.info("Received {} -> {} - {} ({})", HashProducer.toNumberFormat(message.getSenderPeerID()).toString(16), this.myPeerIDHex, message.getMessageID().toString(), message.getMessagePayload().length);
	}

	public static String randomCapitalLetters(int length) {
		int leftLimit = 65; // letter 'A'
		int rightLimit = 90; // letter 'Z'
		Random random = new Random();
		return random.ints(leftLimit, rightLimit + 1).limit(length)
				.collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append).toString();
	}
}
