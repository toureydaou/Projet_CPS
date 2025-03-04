package etape2.test;

import etape1.EntierKey;
import etape1.Livre;
import etape2.composants.ClientBCM;
import etape2.endpoints.DHTServicesEndPoint;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.annotations.RequiredInterfaces;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesCI;

@RequiredInterfaces(required = { DHTServicesCI.class })
public class PutClientTest extends ClientBCM {

	protected PutClientTest(String uri, DHTServicesEndPoint dsep) {
		super(uri, dsep);

	}

	public String getContentData(ContentKeyI key) throws Exception {
		Livre livre = (Livre) this.get(key);
		return livre != null ? (String) livre.getValue(Livre.TITRE) : "Pas de livre à cet emplacement";

	}

	public String putContentData(ContentKeyI key, ContentDataI livre) throws Exception {
		Livre oldLivre = (Livre) this.put(key, livre);

		if (oldLivre == null)
			return null;
		return (String) oldLivre.getValue(Livre.TITRE);
	}

	@Override
	public void execute() throws Exception {
		this.logMessage("executing client component.");

		this.runTask(new AbstractComponent.AbstractTask() {
			@Override
			public void run() {
				try {

					System.out.println(reflectionInboundPortURI);
					((PutClientTest) this.getTaskOwner()).putContentData(new EntierKey(70), new Livre("Socrate", 500));

					// test de l'insertion
					String oldValue = ((PutClientTest) this.getTaskOwner()).putContentData(new EntierKey(70),
							new Livre("Homere", 125));
					System.out.println("Test put : " + "Socrate".equals(oldValue));

					// test de la récupération de données
					String value = ((PutClientTest) this.getTaskOwner()).getContentData(new EntierKey(70));
					System.out.println("Test get : " + "Homere".equals(value));
					
					// insertion d'une donnée en dehors de l'intervalle des clés de la table
					((PutClientTest) this.getTaskOwner()).putContentData(new EntierKey(7500),
							new Livre("Aristote", 750));
					
					Livre oldValue2 = (Livre) ((PutClientTest) this.getTaskOwner()).get(new EntierKey(7500));
					
					System.out.println("Test put out of DHT boundaries : " + (null == oldValue2));
					
					

				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
	}

}
