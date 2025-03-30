package etape3.composants;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

import etape2.endpoints.DHTServicesEndPoint;
import etape3.CVM;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.annotations.RequiredInterfaces;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesCI;
import fr.sorbonne_u.utils.aclocks.ClocksServer;
import fr.sorbonne_u.utils.aclocks.ClocksServerCI;
import fr.sorbonne_u.utils.aclocks.ClocksServerConnector;
import fr.sorbonne_u.utils.aclocks.ClocksServerOutboundPort;


@RequiredInterfaces(required = { DHTServicesCI.class, ClocksServerCI.class })
public class RemoveClient extends ClientBCM {

	protected RemoveClient(String uri, DHTServicesEndPoint endpointClientFacade) {
		super(uri, endpointClientFacade);
	}

	protected void connectToClockServer() throws Exception {
	    ClocksServerOutboundPort p = new ClocksServerOutboundPort(this);
	    p.publishPort();
	    
	    this.doPortConnection(
	        p.getPortURI(),
	        ClocksServer.STANDARD_INBOUNDPORT_URI,
	        ClocksServerConnector.class.getCanonicalName()
	    );
	    
	    this.dhtClock = p.getClock(CVM.TEST_CLOCK_URI);
	    
	    this.doPortDisconnection(p.getPortURI());
	    p.unpublishPort();
	    p.destroyPort();
	    
	    this.logMessage("En attente du démarrage de l'horloge...");
	    dhtClock.waitUntilStart();
	    this.logMessage("Horloge démarrée : " + dhtClock.getStartInstant());
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
					    // Test 1: Suppression de données
					    testSuppression();
				} catch (Exception e) {
					e.printStackTrace();
				} 
			}
		}, delay, TimeUnit.NANOSECONDS);

	}
	
}
