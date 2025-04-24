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

@RequiredInterfaces(required = { DHTServicesCI.class, ClocksServerCI.class })
public class ConcurrentPutClient3 extends ClientBCM {

	
	private static final int STARTING_DELAY = 360;
	
	protected ConcurrentPutClient3(String uri, DHTServicesEndPoint endpointClientFacade) {
		super(uri, endpointClientFacade);
	}

	
	private void ecritureConcurrente() throws Exception {
		
		System.out.println("");

		System.out.println("============= Insertion concurrente des clés 55, 65, 75, 85, 95, 105, 115 (PUT 3)  ==============");

		System.out.println("");

		System.out.println(
				"Client URI :" + this.reflectionInboundPortURI + ", delay :" + ConcurrentPutClient3.STARTING_DELAY);

		System.out.println("");

		System.out.println("Insertion clé 55 (put3)");

		System.out.println("");

		ContentDataI value_55 = this.put(new EntierKey(55), new Livre("Nouveau Harry Potter55 put 2", 700));
		System.out.println("Résultat attendu (PUT-3-55) : null");
		System.out.println("Résultat obtenu (PUT-3-55) : " + value_55);

		System.out.println("");

		System.out.println("Insertion clé 65 (put3)");

		System.out.println("");

		ContentDataI value_65 = this.put(new EntierKey(65), new Livre("Nouveau Harry Potter65 put 2", 700));
		System.out.println("Résultat attendu (PUT-3-65) : null");
		System.out.println("Résultat obtenu (PUT-3-65) : " + value_65);

		System.out.println("");

		System.out.println("Insertion clé 75 (put3)");

		System.out.println("");

		ContentDataI value_75 = this.put(new EntierKey(75), new Livre("Nouveau Harry Potter75 put 2", 700));
		System.out.println("Résultat attendu (PUT-3-75) : null");
		System.out.println("Résultat obtenu (PUT-3-75) : " + value_75);

		System.out.println("");

		System.out.println("Insertion clé 85 (put3)");

		System.out.println("");

		ContentDataI value_85 = this.put(new EntierKey(85), new Livre("Nouveau Harry Potter85 put 2", 700));
		System.out.println("Résultat attendu (PUT-3-85) : null");
		System.out.println("Résultat obtenu (PUT-3-85) : " + value_85);

		System.out.println("");

		System.out.println("Insertion clé 95 (put3)");

		System.out.println("");

		ContentDataI value_95 = this.put(new EntierKey(95), new Livre("Nouveau Harry Potter95 put 2", 700));
		System.out.println("Résultat attendu (PUT-3-95) : null");
		System.out.println("Résultat obtenu (PUT-3-95) : " + value_95);

		System.out.println("");

		System.out.println("Insertion clé 105 (put3)");

		System.out.println("");


		ContentDataI value_105 = this.put(new EntierKey(105), new Livre("Nouveau Harry Potter105 put 2", 700));
		System.out.println("Résultat attendu (PUT-3-105) : null");
		System.out.println("Résultat obtenu (PUT-3-105) : " + value_105);

		System.out.println("");

		System.out.println("Insertion clé 115 (put3)");

		System.out.println("");


		ContentDataI value_115 = this.put(new EntierKey(115), new Livre("Nouveau Harry Potter115 put 2", 700));
		System.out.println("Résultat attendu (PUT-3-115) : null");
		System.out.println("Résultat obtenu (PUT-3-115) : " + value_115);

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
					ecritureConcurrente();
				} catch (Exception e) {
					e.printStackTrace();
				} 
			}
		}, delay, TimeUnit.NANOSECONDS);

	}
	
	
}
