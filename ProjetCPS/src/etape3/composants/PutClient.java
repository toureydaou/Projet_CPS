package etape3.composants;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

import etape1.EntierKey;
import etape1.Livre;
import etape2.endpoints.DHTServicesEndPoint;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.annotations.RequiredInterfaces;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesCI;
import fr.sorbonne_u.utils.aclocks.ClocksServerCI;

/**
 * The Class PutClient.
 * 
 * @author Touré-Ydaou TEOURI
 * @author Awwal FAGBEHOURO
 */
@RequiredInterfaces(required = { DHTServicesCI.class, ClocksServerCI.class })
public class PutClient extends ClientBCM {

	/**
	 * Crée un put client.
	 *
	 * @param uri                  the uri
	 * @param endpointClientFacade the endpoint client facade
	 */
	protected PutClient(String uri, DHTServicesEndPoint endpointClientFacade) {
		super(uri, endpointClientFacade);
	}

	private static final int STARTING_DELAY = 60;

	// --------------------------------------------------------
	// Méthodes de test
	// --------------------------------------------------------

	/**
	 * Teste l'insertion de données dans le DHT.
	 *
	 * @throws Exception the exception
	 */
	private void testInsertion() throws Exception {
		System.out.println("");
		System.out.println("\n=== TEST INSERTION ===");
		System.out.println("");

		System.out.println("Client URI :" + this.reflectionInboundPortURI + ", delay :" + PutClient.STARTING_DELAY);

		System.out.println("");

		EntierKey k10 = new EntierKey(10);
		EntierKey k20 = new EntierKey(20);
		EntierKey k30 = new EntierKey(30);
		EntierKey k40 = new EntierKey(40);
		EntierKey k50 = new EntierKey(50);
		EntierKey k60 = new EntierKey(60);
		EntierKey k70 = new EntierKey(70);
		EntierKey k80 = new EntierKey(80);
		EntierKey k90 = new EntierKey(90);
		EntierKey k100 = new EntierKey(100);
		EntierKey k120 = new EntierKey(120);
		EntierKey k130 = new EntierKey(130);
		EntierKey k600 = new EntierKey(600);

		Livre livreHP_10 = new Livre("Harry Potter1", 100);

		Livre livreHP_20 = new Livre("Harry Potter2", 400);

		Livre livreHP_new_20 = new Livre("Harry Potter3", 300);

		Livre livreHP_new_30 = new Livre("Harry Potter4", 400);

		Livre livreHP_new_40 = new Livre("Harry Potter5", 500);

		Livre livreHP_new_50 = new Livre("Harry Potter6", 600);

		Livre livreHP_new_60 = new Livre("Harry Potter7", 700);

		Livre livreHP_new_70 = new Livre("Harry Potter8", 800);

		Livre livreHP_new_80 = new Livre("Harry Potter9", 900);

		Livre livreHP_new_90 = new Livre("Harry Potter10", 1000);

		Livre livreHP_new_100 = new Livre("Harry Potter11", 1100);

		Livre livreHP_new_120 = new Livre("Harry Potter12", 1200);

		Livre livreHP_new_130 = new Livre("Harry Potter13", 1300);

		System.out.println("Insertion de la clé 10 avec valeur: " + livreHP_10);
		ContentDataI previousValue_10 = this.put(k10, livreHP_10);

		System.out.println("Résultat attendu: null (première insertion)");
		System.out.println("Résultat obtenu: " + previousValue_10);

		System.out.println("");

		System.out.println("Insertion de la clé 20 avec valeur: " + livreHP_20);
		this.put(k20, livreHP_20);
		ContentDataI previousValue_20 = this.put(k20, livreHP_new_20);

		System.out.println("Résultat attendu: Livre[Harry Potter2, 400]");
		System.out.println("Résultat obtenu: " + previousValue_20);

		System.out.println("");

		System.out.println("Insertion de la clé 30 avec valeur: " + livreHP_20);
		this.put(k30, livreHP_20);
		ContentDataI previousValue_30 = this.put(k30, livreHP_new_30);

		System.out.println("Résultat attendu: Livre[Harry Potter2, 400]");
		System.out.println("Résultat obtenu: " + previousValue_30);

		System.out.println("");

		System.out.println("Insertion de la clé 40 avec valeur: " + livreHP_20);
		this.put(k40, livreHP_20);
		ContentDataI previousValue_40 = this.put(k40, livreHP_new_40);

		System.out.println("Résultat attendu: Livre[Harry Potter2, 400]");
		System.out.println("Résultat obtenu: " + previousValue_40);

		System.out.println("");

		System.out.println("Insertion de la clé 50 avec valeur: " + livreHP_20);
		this.put(k50, livreHP_20);
		ContentDataI previousValue_50 = this.put(k50, livreHP_new_50);

		System.out.println("Résultat attendu: Livre[Harry Potter2, 400]");
		System.out.println("Résultat obtenu: " + previousValue_50);

		System.out.println("");

		System.out.println("Insertion de la clé 60 avec valeur: " + livreHP_20);
		this.put(k60, livreHP_20);
		ContentDataI previousValue_60 = this.put(k60, livreHP_new_60);

		System.out.println("Résultat attendu: Livre[Harry Potter2, 400]");
		System.out.println("Résultat obtenu: " + previousValue_60);

		System.out.println("");

		System.out.println("Insertion de la clé 70 avec valeur: " + livreHP_20);
		this.put(k70, livreHP_20);
		ContentDataI previousValue_70 = this.put(k70, livreHP_new_70);

		System.out.println("Résultat attendu: Livre[Harry Potter2, 400]");
		System.out.println("Résultat obtenu: " + previousValue_70);

		System.out.println("");

		System.out.println("Insertion de la clé 80 avec valeur: " + livreHP_20);
		this.put(k80, livreHP_20);
		ContentDataI previousValue_80 = this.put(k80, livreHP_new_80);

		System.out.println("Résultat attendu: Livre[Harry Potter2, 400]");
		System.out.println("Résultat obtenu: " + previousValue_80);

		System.out.println("");

		System.out.println("Insertion de la clé 90 avec valeur: " + livreHP_20);
		this.put(k90, livreHP_20);
		ContentDataI previousValue_90 = this.put(k90, livreHP_new_90);

		System.out.println("Résultat attendu: Livre[Harry Potter2, 400]");
		System.out.println("Résultat obtenu: " + previousValue_90);

		System.out.println("");

		System.out.println("Insertion de la clé 100 avec valeur: " + livreHP_20);
		this.put(k100, livreHP_20);
		ContentDataI previousValue_100 = this.put(k100, livreHP_new_100);

		System.out.println("Résultat attendu: Livre[Harry Potter2, 400]");
		System.out.println("Résultat obtenu: " + previousValue_100);

		System.out.println("");

		System.out.println("Insertion de la clé 120 avec valeur: " + livreHP_20);
		this.put(k120, livreHP_20);
		ContentDataI previousValue_120 = this.put(k120, livreHP_new_120);

		System.out.println("Résultat attendu: Livre[Harry Potter2, 400]");
		System.out.println("Résultat obtenu: " + previousValue_120);

		System.out.println("");

		System.out.println("Insertion de la clé 130 avec valeur: " + livreHP_20);
		this.put(k130, livreHP_20);
		ContentDataI previousValue_130 = this.put(k130, livreHP_new_130);

		System.out.println("Résultat attendu: Livre[Harry Potter2, 400]");
		System.out.println("Résultat obtenu: " + previousValue_130);

		System.out.println("");

		System.out.println("Insertion de la clé 600 avec valeur: " + livreHP_20);
		this.put(k600, livreHP_20);
		ContentDataI previousValue_600 = this.put(k600, livreHP_new_130);

		System.out.println("Résultat attendu: null");
		System.out.println("Résultat obtenu: " + previousValue_600);

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

					testInsertion();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}, delay, TimeUnit.NANOSECONDS);

	}

}
