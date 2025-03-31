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

@RequiredInterfaces(required = { DHTServicesCI.class, ClocksServerCI.class })
public class ConcurrentGetClient1 extends ClientBCM {

	protected ConcurrentGetClient1(String uri, DHTServicesEndPoint endpointClientFacade) {
		super(uri, endpointClientFacade);
	}

	private static final int STARTING_DELAY = 300;

	private void testLectureConcurrente() throws Exception {
		System.out.println("");

		System.out.println("============= Lecture concurrente de la clé 40 (lecture - écriture) ==============");

		System.out.println("");

		System.out.println(
				"Client URI :" + this.reflectionInboundPortURI + ", delay :" + ConcurrentGetClient1.STARTING_DELAY);

		System.out.println("");

		ContentDataI value_40 = this.get(new EntierKey(40));
		System.out.println("Résultat attendu (GET) : Livre[Harry Potter5, 500]");
		System.out.println("Résultat obtenu (GET): " + value_40);

		System.out.println("");
	}

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

					testLectureConcurrente();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}, delay, TimeUnit.NANOSECONDS);

	}

}
