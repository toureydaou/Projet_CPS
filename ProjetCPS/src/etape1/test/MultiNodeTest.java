package etape1.test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import etape1.Client;
import etape1.EntierKey;
import etape1.Facade;
import etape1.Livre;
import etape1.Node;
import fr.sorbonne_u.components.endpoints.POJOEndPoint;
import fr.sorbonne_u.components.exceptions.ConnectionException;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesI;
import fr.sorbonne_u.cps.mapreduce.endpoints.POJOContentNodeCompositeEndPoint;
import fr.sorbonne_u.cps.mapreduce.utils.IntInterval;

public class MultiNodeTest {

	// clés de la DHT
	private EntierKey k_10;
	private EntierKey k_40;
	private EntierKey k_60;
	private EntierKey k_80;
	private EntierKey k_225;

	// données de la DHT
	private Livre livre_1;
	private Livre livre_2;
	private Livre livre_3;
	private Livre livre_4;
	private Livre livre_5;
	private Livre livre_6;
	private Livre livre_7;

	private static final String LIVRE_1_NOM = "Odysee";
	private static final String LIVRE_2_NOM = "Horla";
	private static final String LIVRE_3_NOM = "Un piege sans fin";
	private static final String LIVRE_4_NOM = "Miserables";
	private static final String LIVRE_5_NOM = "Petit Nicolas";
	private static final String LIVRE_6_NOM = "Ville Cruelle";
	private static final String LIVRE_7_NOM = "Aventure Ambigue";

	private static final int LIVRE_1_PAGES = 500;
	private static final int LIVRE_2_PAGES = 350;
	private static final int LIVRE_3_PAGES = 125;
	private static final int LIVRE_4_PAGES = 450;
	private static final int LIVRE_5_PAGES = 110;
	private static final int LIVRE_6_PAGES = 150;
	private static final int LIVRE_7_PAGES = 135;

	private static final int NOMBRE_NOEUDS = 4;
	private static final int NOMBRE_ENDPOINTS = 4;

	private static final int somme_pages = LIVRE_1_PAGES + LIVRE_2_PAGES + LIVRE_3_PAGES + LIVRE_4_PAGES;

	private Node[] noeuds = new Node[NOMBRE_NOEUDS];
	private POJOContentNodeCompositeEndPoint[] pojoEndpoints = new POJOContentNodeCompositeEndPoint[NOMBRE_ENDPOINTS];

	Client client;

	Facade facade;

	@BeforeEach
	public void initialise() throws ConnectionException {
		k_10 = new EntierKey(10);
		k_40 = new EntierKey(40);
		k_60 = new EntierKey(60);
		k_80 = new EntierKey(80);
		k_225 = new EntierKey(225);

		livre_1 = new Livre(LIVRE_1_NOM, LIVRE_1_PAGES);
		livre_2 = new Livre(LIVRE_2_NOM, LIVRE_2_PAGES);
		livre_3 = new Livre(LIVRE_3_NOM, LIVRE_3_PAGES);
		livre_4 = new Livre(LIVRE_4_NOM, LIVRE_4_PAGES);
		livre_5 = new Livre(LIVRE_5_NOM, LIVRE_5_PAGES);
		livre_6 = new Livre(LIVRE_6_NOM, LIVRE_6_PAGES);
		livre_7 = new Livre(LIVRE_7_NOM, LIVRE_7_PAGES);

		for (int i = 0; i < NOMBRE_ENDPOINTS; i++) {
			pojoEndpoints[i] = new POJOContentNodeCompositeEndPoint();
		}

	

		// endpoint client - facade
		POJOEndPoint<DHTServicesI> endpointClientFacade = new POJOEndPoint<DHTServicesI>(DHTServicesI.class);

		// connexion client - facade
		client = new Client(endpointClientFacade);
		facade = new Facade((POJOContentNodeCompositeEndPoint) pojoEndpoints[0], endpointClientFacade);

		// connexion des noeuds entre eux
		for (int i = 0; i < NOMBRE_NOEUDS; i++) {
			int first = i * 50;
			int last = ((i + 1)  * 50) -1;
			if (i == 3) {
				// connexion du dernier noeud au premier
				noeuds[i] = new Node(new IntInterval(first, last), pojoEndpoints[i], pojoEndpoints[0]);
			} else {
				// connexion des autres noeuds
				noeuds[i] = new Node(new IntInterval(first, last), pojoEndpoints[i], pojoEndpoints[i+1]);
			}
			
			
		}


	}

	@Test
	public void putInHashMaptest() throws Exception {
		client.put(k_10, livre_1);
		client.put(k_10, livre_2);
		assertEquals(LIVRE_2_NOM, client.get(k_10).getValue(Livre.TITRE));
	}

	@Test
	public void putOutOfBoundsInHashMaptest() throws Exception {
		client.put(k_225, livre_1);
		assertEquals(null, client.get(k_225));
	}

	@Test
	public void getOutOfBoundsInHashMaptest() throws Exception {
		client.put(k_225, livre_1);
		assertEquals(null, client.get(k_225));
	}

	@Test
	public void removeOutOfBoundsInHashMaptest() throws Exception {
		client.put(k_225, livre_1);
		assertEquals(null, client.remove(k_225));
	}

	@Test
	public void put2InHashMaptest() throws Exception {
		client.put(k_10, livre_1);
		assertEquals(LIVRE_1_NOM, client.put(k_10, livre_2).getValue(Livre.TITRE));
	}

	@Test
	public void getFromHashMaptest() throws Exception {
		client.put(k_10, livre_1);
		assertEquals(LIVRE_1_NOM, client.get(k_10).getValue(Livre.TITRE));
	}

	@Test
	public void removeFromHashMaptest() throws Exception {
		client.put(k_10, livre_1);
		client.remove(k_10).getValue(Livre.TITRE);
		assertEquals(null, client.get(k_10));
	}

	@Test
	public void remove2FromHashMaptest() throws Exception {
		client.put(k_10, livre_1);
		assertEquals(LIVRE_1_NOM, client.remove(k_10).getValue(Livre.TITRE));

	}

	@Test
	public void mapReduceOnHashMaptest() throws Exception {
		client.put(k_10, livre_1);
		client.put(k_40, livre_2);
		client.put(k_60, livre_3);
		client.put(k_80, livre_4);
		int total_pages = client.mapReduce(i -> ((int) i.getValue(Livre.NB_PAGES)) > 0,
				i -> new Livre((String) i.getValue(Livre.TITRE), (int) i.getValue(Livre.NB_PAGES)),
				(acc, i) -> (acc + (int) i.getValue(Livre.NB_PAGES)), (acc, i) -> (acc + i), 0);

		assertEquals(somme_pages, total_pages);
	}

}
