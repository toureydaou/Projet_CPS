package etape4.composants;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

import etape1.EntierKey;
import etape1.Livre;
import etape2.endpoints.DHTServicesEndPoint;
import etape3.composants.ClientBCM;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.annotations.RequiredInterfaces;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesCI;
import fr.sorbonne_u.utils.aclocks.ClocksServerCI;

/**
 * Composant client permettant de forcer la fusion (merge) de nœuds 
 * dans la DHT en insérant un certain nombre de livres.
 */
@RequiredInterfaces(required = { DHTServicesCI.class, ClocksServerCI.class })
public class ForceMergeClient extends ClientBCM {
	
	private static final int STARTING_DELAY = 480;
	
	/** Crée un client qui permettra de forcer un merge sur le noeud
	 * @param uri
	 * @param endpointClientFacade
	 */
	protected ForceMergeClient(String uri, DHTServicesEndPoint endpointClientFacade) {
		super(uri, endpointClientFacade);
	}
	
	/** Insère 10 livres dans un noeud et 15 dans un autres pour déclencler un merge 
	 * @throws Exception
	 */
	private void forceMerge() throws Exception {
		
		System.out.println("");

		System.out.println("============= Forçage du merge sur les noeuds  ==============");

		System.out.println("");

		for (int i = 500; i < 510; i++ ) {
			System.out.println("");

			System.out.println("============= Insertion de la clée " + i +  "  ==============");

			System.out.println("");
			this.put(new EntierKey(i), new Livre("Nouveau Harry Potter" + i, 700 + (i*10)));
		}
		
		for (int i = 550; i < 555; i++ ) {
			System.out.println("");

			System.out.println("============= Insertion de la clée " + i +  "  ==============");

			System.out.println("");
			this.put(new EntierKey(i), new Livre("Nouveau Harry Potter" + i, 700 + (i*10)));
		}

	}
		
	/**
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
					forceMerge();
				} catch (Exception e) {
					e.printStackTrace();
				} 
			}
		}, delay, TimeUnit.NANOSECONDS);

	}
	
	
}
