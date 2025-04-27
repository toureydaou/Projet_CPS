package etape3.composants;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

import etape1.EntierKey;
import etape2.endpoints.DHTServicesEndPoint;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.annotations.RequiredInterfaces;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesCI;
import fr.sorbonne_u.utils.aclocks.ClocksServerCI;

/**
 * La Classe GetClient.
 * 
 * @author Touré-Ydaou TEOURI
 * @author Awwal FAGBEHOURO
 */
@RequiredInterfaces(required = { DHTServicesCI.class, ClocksServerCI.class })
public class GetClient extends ClientBCM {

	private static final int STARTING_DELAY = 120;

	/**
	 * Instantiates a new gets the client.
	 *
	 * @param uri                  the uri
	 * @param endpointClientFacade the endpoint client facade
	 */
	protected GetClient(String uri, DHTServicesEndPoint endpointClientFacade) {
		super(uri, endpointClientFacade);
	}

	/**
	 * Teste la récupération de données depuis le DHT.
	 *
	 * @throws Exception the exception
	 */
	private void testRecuperation() throws Exception {

		System.out.println("");
		System.out.println("\n=== TEST RECUPERATION ===");
		System.out.println("");

		System.out.println("Client URI :" + this.reflectionInboundPortURI + ", delay :" + GetClient.STARTING_DELAY);

		System.out.println("Récupération de la clé 10");

		ContentDataI value_10 = this.get(new EntierKey(10));
		System.out.println("Résultat attendu: Livre[Harry Potter1, 200]");
		System.out.println("Résultat obtenu: " + value_10);

		System.out.println("");

		System.out.println("Récupération de la clé 20");

		ContentDataI value_20 = this.get(new EntierKey(20));
		System.out.println("Résultat attendu: Livre[Harry Potter3, 200]");
		System.out.println("Résultat obtenu: " + value_20);

		System.out.println("");

		System.out.println("Récupération de la clé 30");

		ContentDataI value_30 = this.get(new EntierKey(30));
		System.out.println("Résultat attendu: Livre[Harry Potter4, 200]");
		System.out.println("Résultat obtenu: " + value_30);

		System.out.println("");

		System.out.println("Récupération de la clé 40");

		ContentDataI value_40 = this.get(new EntierKey(40));
		System.out.println("Résultat attendu: Livre[Harry Potter5, 200]");
		System.out.println("Résultat obtenu: " + value_40);

		System.out.println("");

		System.out.println("Récupération de la clé 50");

		ContentDataI value_50 = this.get(new EntierKey(50));
		System.out.println("Résultat attendu: Livre[Harry Potter6, 200]");
		System.out.println("Résultat obtenu: " + value_50);

		System.out.println("");

		System.out.println("Récupération de la clé 60");

		ContentDataI value_60 = this.get(new EntierKey(60));
		System.out.println("Résultat attendu: Livre[Harry Potter7, 200]");
		System.out.println("Résultat obtenu: " + value_60);

		System.out.println("");

		System.out.println("Récupération de la clé 70");

		ContentDataI value_70 = this.get(new EntierKey(70));
		System.out.println("Résultat attendu: Livre[Harry Potter8, 200]");
		System.out.println("Résultat obtenu: " + value_70);

		System.out.println("");

		System.out.println("Récupération de la clé 80");

		ContentDataI value_80 = this.get(new EntierKey(80));
		System.out.println("Résultat attendu: Livre[Harry Potter9, 200]");
		System.out.println("Résultat obtenu: " + value_80);

		System.out.println("");

		System.out.println("Récupération de la clé 90");

		ContentDataI value_90 = this.get(new EntierKey(90));
		System.out.println("Résultat attendu: Livre[Harry Potter10, 200]");
		System.out.println("Résultat obtenu: " + value_90);

		System.out.println("");

		System.out.println("Récupération de la clé 100");

		ContentDataI value_100 = this.get(new EntierKey(100));
		System.out.println("Résultat attendu: Livre[Harry Potter11, 200]");
		System.out.println("Résultat obtenu: " + value_100);

		System.out.println("");

		System.out.println("Récupération de la clé 120");

		ContentDataI value_120 = this.get(new EntierKey(120));
		System.out.println("Résultat attendu: Livre[Harry Potter12, 200]");
		System.out.println("Résultat obtenu: " + value_120);

		System.out.println("");

		System.out.println("Récupération de la clé 130");

		ContentDataI value_130 = this.get(new EntierKey(130));
		System.out.println("Résultat attendu: Livre[Harry Potter13, 200]");
		System.out.println("Résultat obtenu: " + value_130);

		System.out.println("");

		System.out.println("Récupération de la clé 300");

		ContentDataI value_300 = this.get(new EntierKey(300));
		System.out.println("Résultat attendu: null");
		System.out.println("Résultat obtenu: " + value_300);

		System.out.println("");

		System.out.println("Récupération de la clé 500");

		ContentDataI value_500 = this.get(new EntierKey(500));
		System.out.println("Résultat attendu: null");
		System.out.println("Résultat obtenu: " + value_500);

		System.out.println("");

		System.out.println("Récupération de la clé 600");

		ContentDataI value_600 = this.get(new EntierKey(600));
		System.out.println("Résultat attendu: null");
		System.out.println("Résultat obtenu: " + value_600);

	}

	/**
	 * 
	 * @see etape3.composants.ClientBCM#execute()
	 */
	@Override
	public void execute() throws Exception {
		this.logMessage("executing client component." + isStarted());

		Instant i0 = dhtClock.getStartInstant();
		Instant i1 = i0.plusSeconds(STARTING_DELAY);

		long delay = dhtClock.nanoDelayUntilInstant(i1);

		this.scheduleTask(new AbstractComponent.AbstractTask() {
			@Override
			public void run() {
				try {
					// Test 1: Récupération de données
					testRecuperation();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}, delay, TimeUnit.NANOSECONDS);

	}

}
