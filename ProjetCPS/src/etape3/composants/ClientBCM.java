package etape3.composants;

import java.io.Serializable;

import etape1.EntierKey;
import etape1.Livre;
import etape2.endpoints.DHTServicesEndPoint;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.annotations.RequiredInterfaces;
import fr.sorbonne_u.components.exceptions.ComponentStartException;
import fr.sorbonne_u.components.exceptions.ConnectionException;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;

/**
 * ClientBCM est un composant client qui interagit avec le service DHT pour
 * effectuer des opérations de type MapReduce, ainsi que des opérations CRUD sur
 * les données stockées dans le DHT.
 * 
 * @author Touré-Ydaou TEOURI
 * @author Awwal FAGBEHOURO
 */

@RequiredInterfaces(required = { DHTServicesCI.class })
public class ClientBCM extends AbstractComponent {

	protected DHTServicesEndPoint endPointClientFacade; // Point d'accès aux services DHT

	private static final int SCHEDULABLE_THREADS = 1;
	private static final int THREADS_NUMBER = 0;

	/**
	 * Constructeur du composant ClientBCM.
	 * 
	 * @param uri                  URI du composant.
	 * @param endpointClientFacade Point d'accès aux services DHT.
	 */
	protected ClientBCM(String uri, DHTServicesEndPoint endpointClientFacade) {
		super(uri, THREADS_NUMBER, SCHEDULABLE_THREADS);
		this.endPointClientFacade = endpointClientFacade;

	}

	/**
	 * Récupère une donnée à partir de la clé fournie.
	 * 
	 * @param key Clé de la donnée à récupérer.
	 * @return La donnée correspondante à la clé.
	 * @throws Exception Si une erreur se produit lors de la récupération.
	 */
	public ContentDataI get(ContentKeyI key) throws Exception {
		System.out.println("Envoi de la requête 'GET' sur la facade");
		return this.endPointClientFacade.getClientSideReference().get(key);
	}

	/**
	 * Ajoute ou met à jour une donnée associée à une clé dans le DHT.
	 * 
	 * @param key   La clé associée à la donnée.
	 * @param value La donnée à stocker.
	 * @return La donnée précédente associée à la clé (ou null si c'est un nouvel
	 *         élément).
	 * @throws Exception Si une erreur se produit lors de l'ajout.
	 */
	public ContentDataI put(ContentKeyI key, ContentDataI value) throws Exception {
		System.out.println("Envoi de la requête 'PUT' sur la facade");
		return this.endPointClientFacade.getClientSideReference().put(key, value);
	}

	/**
	 * Supprime une donnée associée à une clé dans le DHT.
	 * 
	 * @param key La clé associée à la donnée à supprimer.
	 * @return La donnée supprimée.
	 * @throws Exception Si une erreur se produit lors de la suppression.
	 */
	public ContentDataI remove(ContentKeyI key) throws Exception {
		System.out.println("Envoi de la requête 'REMOVE' sur la facade");
		return this.endPointClientFacade.getClientSideReference().remove(key);
	}

	/**
	 * Effectue une opération MapReduce sur les données stockées dans le DHT.
	 * 
	 * @param selector   Un sélecteur pour filtrer les données.
	 * @param processor  Un processeur pour transformer les données.
	 * @param reductor   Un réducteur pour agréger les résultats.
	 * @param combinator Un combinator pour combiner les résultats intermédiaires.
	 * @param initialAcc L'accumulateur initial.
	 * @return Le résultat final de l'opération MapReduce.
	 * @throws Exception Si une erreur se produit lors de l'exécution.
	 */
	public <R extends Serializable, A extends Serializable> A mapReduce(SelectorI selector, ProcessorI<R> processor,
			ReductorI<A, R> reductor, CombinatorI<A> combinator, A initialAcc) throws Exception {
		System.out.println("Envoi de la requête 'MAP REDUCE' sur la facade");
		return this.endPointClientFacade.getClientSideReference().mapReduce(selector, processor, reductor, combinator,
				initialAcc);
	}

	@Override
	public void start() throws ComponentStartException {
		this.logMessage("starting client component.");
		super.start();
		try {
			if (!endPointClientFacade.clientSideInitialised()) {
				this.endPointClientFacade.initialiseClientSide(this);
			}
		} catch (ConnectionException e) {
			throw new ComponentStartException(e);
		}
	}

	@Override
	public void execute() throws Exception {
		this.logMessage("executing client component." + isStarted());

		this.runTask(new AbstractComponent.AbstractTask() {
			@Override
			public void run() {
				try {
					EntierKey k_10 = new EntierKey(10);
					EntierKey k_50 = new EntierKey(50);
					EntierKey k_100 = new EntierKey(100);
					EntierKey k_500 = new EntierKey(500);
				
					((ClientBCM) this.taskOwner).put(k_10, new Livre("Harry potter", 200));
					((ClientBCM) this.taskOwner).put(k_50, new Livre("Harry potter", 200));
					((ClientBCM) this.taskOwner).put(k_100, new Livre("Harry potter", 200));
					System.out.println(((ClientBCM) this.taskOwner).get(k_500));
				
					
					int reduce = ((ClientBCM) this.taskOwner).mapReduce(i -> ((int) i.getValue(Livre.NB_PAGES)) > 0,
							i -> new Livre((String) i.getValue(Livre.TITRE), (int) i.getValue(Livre.NB_PAGES)),
							(acc, i) -> (acc + (int) i.getValue(Livre.NB_PAGES)), (acc, i) -> (acc + i), 0);

					System.out.println("Map reduce : " + reduce);
				} catch (Exception e) {
					e.printStackTrace();
				} finally {

				}
			}
		});
	}

	@Override
	public void finalise() throws Exception {
		this.logMessage("stopping client component.");
		this.printExecutionLogOnFile("client");

		this.endPointClientFacade.cleanUpClientSide();

		super.finalise();
	}

}
