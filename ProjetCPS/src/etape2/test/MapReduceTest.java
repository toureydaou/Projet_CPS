package etape2.test;

import etape1.EntierKey;
import etape1.Livre;
import etape2.composants.ClientBCM;
import etape2.endpoints.DHTServicesEndPoint;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.annotations.RequiredInterfaces;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesCI;

@RequiredInterfaces(required = { DHTServicesCI.class })
public class MapReduceTest extends ClientBCM {

	protected MapReduceTest(String uri, DHTServicesEndPoint dsep) {
		super(uri, dsep);

	}

	public int mapReduceTest() throws Exception {
		EntierKey k_22 = new EntierKey(22);
		EntierKey k_120 = new EntierKey(120);
		this.put(k_22, new Livre("soif", 100));
		this.put(k_120, new Livre("douleur", 50));

		int a = this.mapReduce(i -> ((int) i.getValue(Livre.NB_PAGES)) > 0,
				i -> new Livre((String) i.getValue(Livre.TITRE), (int) i.getValue(Livre.NB_PAGES) / 2),
				(acc, i) -> (acc + (int) i.getValue(Livre.NB_PAGES)), (acc, i) -> (acc + i), 0);

		return a;
	}

	@Override
	public void execute() throws Exception {
		this.logMessage("executing client component.");

		this.runTask(new AbstractComponent.AbstractTask() {
			@Override
			public void run() {
				try {

					System.out.println(reflectionInboundPortURI);
					int result = ((MapReduceTest) this.getTaskOwner()).mapReduceTest();
					System.out.println("Test map reduce : " + (result == 75));

				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
	}

}
