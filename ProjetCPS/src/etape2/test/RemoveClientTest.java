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
public class RemoveClientTest extends ClientBCM {

	protected RemoveClientTest(String uri, DHTServicesEndPoint dsep) {
		super(uri, dsep);

	}

	public String removeContentData(ContentKeyI key) throws Exception {
		Livre oldLivre = (Livre) this.remove(key);
		this.logMessage("Titre du livre : " + oldLivre.getValue(Livre.TITRE));
		return (String) oldLivre.getValue(Livre.TITRE);

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
					((RemoveClientTest) this.getTaskOwner()).putContentData(new EntierKey(70),
							new Livre("Socrate", 500));

					// test de la suppression des données
					String removedValue = ((RemoveClientTest) this.getTaskOwner()).removeContentData(new EntierKey(70));
					System.out.println("Test remove : " + "Socrate".equals(removedValue));
					
					// suppression d'une donnée en dehors de la table
					Livre removeValue2 = (Livre) ((RemoveClientTest) this.getTaskOwner()).remove(new EntierKey(300));
					System.out.println("Test remove out of bounds : " + (null == removeValue2));

				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
	}

}
