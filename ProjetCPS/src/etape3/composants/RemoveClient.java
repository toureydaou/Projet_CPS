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
 * La Classe RemoveClient.
 * 
 * @author Touré-Ydaou TEOURI
 * @author Awwal FAGBEHOURO
 */
@RequiredInterfaces(required = { DHTServicesCI.class, ClocksServerCI.class })
public class RemoveClient extends ClientBCM {

	/** The Constant STARTING_DELAY. */
	private static final int STARTING_DELAY = 240;

	/**
	 * Instantiates a new removes the client.
	 *
	 * @param uri                  the uri
	 * @param endpointClientFacade the endpoint client facade
	 */
	protected RemoveClient(String uri, DHTServicesEndPoint endpointClientFacade) {
		super(uri, endpointClientFacade);
	}

	/**
	 * Teste la suppression de données dans le DHT.
	 *
	 * @throws Exception the exception
	 */
	protected void testSuppression() throws Exception {
		System.out.println("\n=== TEST SUPPRESSION ===");

		System.out.println("");

		System.out.println("Client URI :" + this.reflectionInboundPortURI + ", delay :" + RemoveClient.STARTING_DELAY);

		System.out.println("");

		EntierKey k80 = new EntierKey(80);
		System.out.println("Suppression de la clé 80");

		ContentDataI deletedValue_80 = this.remove(k80);
		System.out.println("Résultat attendu: Livre[Harry Potter9, 900]");
		System.out.println("Résultat obtenu: " + deletedValue_80);

		System.out.println("");

		EntierKey k90 = new EntierKey(90);
		System.out.println("Suppression de la clé 90");

		ContentDataI deletedValue_90 = this.remove(k90);
		System.out.println("Résultat attendu: Livre[Harry Potter10, 1000]");
		System.out.println("Résultat obtenu: " + deletedValue_90);

		System.out.println("");

		EntierKey k100 = new EntierKey(100);
		System.out.println("Suppression de la clé 100");

		ContentDataI deletedValue_100 = this.remove(k100);
		System.out.println("Résultat attendu: Livre[Harry Potter11, 1100]");
		System.out.println("Résultat obtenu: " + deletedValue_100);

		System.out.println("");

		EntierKey k120 = new EntierKey(120);
		System.out.println("Suppression de la clé 120");

		ContentDataI deletedValue_120 = this.remove(k120);
		System.out.println("Résultat attendu: Livre[Harry Potter12, 1200]");
		System.out.println("Résultat obtenu: " + deletedValue_120);

		System.out.println("");

		EntierKey k130 = new EntierKey(120);
		System.out.println("Suppression de la clé 130");

		ContentDataI deletedValue_130 = this.remove(k130);
		System.out.println("Résultat attendu: Livre[Harry Potter13, 1300]");
		System.out.println("Résultat obtenu: " + deletedValue_130);

		System.out.println("");

		ContentDataI deletedValue_400 = this.remove(k130);
		System.out.println("Résultat attendu: null");
		System.out.println("Résultat obtenu: " + deletedValue_400);

		System.out.println("");

		ContentDataI deletedValue_900 = this.remove(k130);
		System.out.println("Résultat attendu: null");
		System.out.println("Résultat obtenu: " + deletedValue_900);

		System.out.println("");

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
					// Test 1: Suppression de données
					testSuppression();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}, delay, TimeUnit.NANOSECONDS);

	}

}
