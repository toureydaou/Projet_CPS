package etape3.composants;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

import etape2.endpoints.DHTServicesEndPoint;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.annotations.RequiredInterfaces;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesCI;
import fr.sorbonne_u.utils.aclocks.ClocksServerCI;

@RequiredInterfaces(required = { DHTServicesCI.class, ClocksServerCI.class })
public class GetClient1 extends ClientBCM {

	protected GetClient1(String uri, DHTServicesEndPoint endpointClientFacade) {
		super(uri, endpointClientFacade);
	}

	
	@Override
	public void execute() throws Exception {
		this.logMessage("executing client component." + isStarted());
		
		Instant i0 = dhtClock.getStartInstant();
		Instant i1 = i0.plusSeconds(60);
		
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
