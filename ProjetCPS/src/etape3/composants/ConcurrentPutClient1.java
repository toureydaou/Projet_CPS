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

// TODO: Auto-generated Javadoc
/**
 * The Class ConcurrentPutClient1.
 */
@RequiredInterfaces(required = { DHTServicesCI.class, ClocksServerCI.class })
public class ConcurrentPutClient1 extends ClientBCM {

	/** The Constant STARTING_DELAY. */
	private static final int STARTING_DELAY = 300;

	/**
	 * Instantiates a new concurrent put client 1.
	 *
	 * @param uri                  the uri
	 * @param endpointClientFacade the endpoint client facade
	 */
	protected ConcurrentPutClient1(String uri, DHTServicesEndPoint endpointClientFacade) {
		super(uri, endpointClientFacade);
	}

	/**
	 * Ecriture concurrente.
	 *
	 * @throws Exception the exception
	 */
	private void ecritureConcurrente() throws Exception {

		System.out.println("");

		System.out.println("============= Insertion concurrente de la clé 40 (lecture - écriture) ==============");

		System.out.println("");

		System.out.println(
				"Client URI :" + this.reflectionInboundPortURI + ", delay :" + ConcurrentPutClient1.STARTING_DELAY);

		System.out.println("");

		ContentDataI value_40 = this.put(new EntierKey(40), new Livre("Nouveau Harry Potter5", 700));
		System.out.println("Résultat attendu (PUT) : Livre[Harry Potter5, 500]");
		System.out.println("Résultat obtenu (PUT) : " + value_40);

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
