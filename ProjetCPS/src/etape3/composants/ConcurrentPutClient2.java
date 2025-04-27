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
 * La Classe ConcurrentPutClient2.
 * 
 * @author Touré-Ydaou TEOURI
 * @author Awwal FAGBEHOURO
 */
@RequiredInterfaces(required = { DHTServicesCI.class, ClocksServerCI.class })
public class ConcurrentPutClient2 extends ClientBCM {

	private static final int STARTING_DELAY = 360;

	/**
	 * Crée un nouveau concurrent put client 2.
	 *
	 * @param uri                  the uri
	 * @param endpointClientFacade the endpoint client facade
	 */
	protected ConcurrentPutClient2(String uri, DHTServicesEndPoint endpointClientFacade) {
		super(uri, endpointClientFacade);
	}

	/**
	 * Ecriture concurrente.
	 *
	 * @throws Exception the exception
	 */
	private void ecritureConcurrente() throws Exception {

		System.out.println("");

		System.out.println(
				"============= Insertion concurrente des clés 55, 65, 75, 85, 95, 105, 115 (PUT 2)  ==============");

		System.out.println("");

		System.out.println(
				"Client URI :" + this.reflectionInboundPortURI + ", delay :" + ConcurrentPutClient2.STARTING_DELAY);

		System.out.println("");

		System.out.println("Insertion clé 55 (put2)");

		System.out.println("");

		ContentDataI value_55 = this.put(new EntierKey(55), new Livre("Nouveau Harry Potter55 put 1", 700));
		System.out.println("Résultat attendu (PUT-2-55) : null");
		System.out.println("Résultat obtenu (PUT-2-55) : " + value_55);

		System.out.println("");

		System.out.println("Insertion clé 65 (put2)");

		System.out.println("");

		ContentDataI value_65 = this.put(new EntierKey(65), new Livre("Nouveau Harry Potter65 put 1", 700));
		System.out.println("Résultat attendu (PUT-2-65) : null");
		System.out.println("Résultat obtenu (PUT-2-65) : " + value_65);

		System.out.println("");

		System.out.println("Insertion clé 75 (put2)");

		System.out.println("");

		ContentDataI value_75 = this.put(new EntierKey(75), new Livre("Nouveau Harry Potter75 put 1", 700));
		System.out.println("Résultat attendu (PUT-2-75) : null");
		System.out.println("Résultat obtenu (PUT-2-75) : " + value_75);

		System.out.println("");

		System.out.println("Insertion clé 85 (put2)");

		System.out.println("");

		ContentDataI value_85 = this.put(new EntierKey(85), new Livre("Nouveau Harry Potter85 put 1", 700));
		System.out.println("Résultat attendu (PUT-2-85) : null");
		System.out.println("Résultat obtenu (PUT-2-85) : " + value_85);

		System.out.println("");

		System.out.println("Insertion clé 95 (put2)");

		System.out.println("");

		ContentDataI value_95 = this.put(new EntierKey(95), new Livre("Nouveau Harry Potter95 put 1", 700));
		System.out.println("Résultat attendu (PUT-2-95) : null");
		System.out.println("Résultat obtenu (PUT-2-95) : " + value_95);

		System.out.println("");

		System.out.println("Insertion clé 105 (put2)");

		System.out.println("");

		ContentDataI value_105 = this.put(new EntierKey(105), new Livre("Nouveau Harry Potter105 put 1", 700));
		System.out.println("Résultat attendu (PUT-2-105) : null");
		System.out.println("Résultat obtenu (PUT-2-105) : " + value_105);

		System.out.println("");

		System.out.println("Insertion clé 115 (put2)");

		System.out.println("");

		ContentDataI value_115 = this.put(new EntierKey(115), new Livre("Nouveau Harry Potter115 put 1", 700));
		System.out.println("Résultat attendu (PUT-2-115) : null");
		System.out.println("Résultat obtenu (PUT-2-115) : " + value_115);

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
					ecritureConcurrente();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}, delay, TimeUnit.NANOSECONDS);

	}

}
