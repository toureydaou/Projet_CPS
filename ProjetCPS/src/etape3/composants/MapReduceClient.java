package etape3.composants;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

import etape1.Livre;
import etape2.endpoints.DHTServicesEndPoint;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.annotations.RequiredInterfaces;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesCI;
import fr.sorbonne_u.utils.aclocks.ClocksServerCI;

/**
 * The Class MapReduceClient.
 * 
 * @author Touré-Ydaou TEOURI
 * @author Awwal FAGBEHOURO
 */
@RequiredInterfaces(required = { DHTServicesCI.class, ClocksServerCI.class })
public class MapReduceClient extends ClientBCM {

	/**
	 * Crée un  map reduce client.
	 *
	 * @param uri                  the uri
	 * @param endpointClientFacade the endpoint client facade
	 */
	protected MapReduceClient(String uri, DHTServicesEndPoint endpointClientFacade) {
		super(uri, endpointClientFacade);
	}

	/** The Constant STARTING_DELAY. */
	private static final int STARTING_DELAY = 180;

	// --------------------------------------------------------
	// Méthodes de test
	// --------------------------------------------------------

	/**
	 * Teste l'opération MapReduce.
	 *
	 * @throws Exception the exception
	 */
	protected void testMapReduce() throws Exception {
		System.out.println("\n=== TEST MAPREDUCE ===");

		System.out
				.println("Client URI :" + this.reflectionInboundPortURI + ", delay :" + MapReduceClient.STARTING_DELAY);

		System.out.println("Calcul du total des pages de tous les livres ayant plus de 700 pages");
		int totalPages = this.mapReduce(i -> ((int) i.getValue(Livre.NB_PAGES)) > 700,
				i -> new Livre((String) i.getValue(Livre.TITRE), (int) i.getValue(Livre.NB_PAGES)),
				(acc, i) -> acc + (int) i.getValue(Livre.NB_PAGES), (acc1, acc2) -> acc1 + acc2, 0);

		System.out.println("Client URI : " + this.reflectionInboundPortURI + " Résultat attendu: 6300");
		System.out.println("Client URI : " + this.reflectionInboundPortURI + " Résultat obtenu: " + totalPages);

		if (totalPages != 6300) {
			System.err.println("ERREUR: Le calcul MapReduce est incorrect");
		}
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

					testMapReduce();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}, delay, TimeUnit.NANOSECONDS);

	}

}
