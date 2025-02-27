package etape2.composants;

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

	protected DHTServicesEndPoint dsep; // Point d'accès aux services DHT

	/**
	 * Constructeur du composant ClientBCM.
	 * 
	 * @param uri       URI du composant.
	 * @param dsep      Point d'accès aux services DHT.
	 * @param semaphore Sémaphore pour gérer l'accès concurrent.
	 */
	protected ClientBCM(String uri, DHTServicesEndPoint dsep) {
		super(uri, 0, 2);
		this.dsep = dsep;

	}

	/**
	 * Récupère une donnée à partir de la clé fournie.
	 * 
	 * @param key Clé de la donnée à récupérer.
	 * @return La donnée correspondante à la clé.
	 * @throws Exception Si une erreur se produit lors de la récupération.
	 */
	public ContentDataI get(ContentKeyI key) throws Exception {
		return this.dsep.getClientSideReference().get(key);
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
		return this.dsep.getClientSideReference().put(key, value);
	}

	/**
	 * Supprime une donnée associée à une clé dans le DHT.
	 * 
	 * @param key La clé associée à la donnée à supprimer.
	 * @return La donnée supprimée.
	 * @throws Exception Si une erreur se produit lors de la suppression.
	 */
	public ContentDataI remove(ContentKeyI key) throws Exception {
		return this.dsep.getClientSideReference().remove(key);
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
		return this.dsep.getClientSideReference().mapReduce(selector, processor, reductor, combinator, initialAcc);
	}

	/**
     * Méthode pour ajouter une donnée et l'afficher ensuite.
     * 
     * 
     * @throws Exception Si une erreur se produit lors de l'ajout ou de l'affichage.
     */
	public void getAndPrintContentData() throws Exception {
		EntierKey k_125 = new EntierKey(125);
		this.put(k_125, new Livre("famille", 300));
		Livre livre = (Livre) this.get(k_125);
		System.out.println(livre.getValue(Livre.TITRE));

	}

	/**
     * Méthode pour exécuter une opération MapReduce et afficher le résultat.
     * 
     * @throws Exception Si une erreur se produit lors de l'exécution de MapReduce.
     */
	public void printMapReduceResult() throws Exception {
		EntierKey k_22 = new EntierKey(22);
		EntierKey k_120 = new EntierKey(120);
		this.put(k_22, new Livre("soif", 100));
		this.put(k_120, new Livre("douleur", 50));

		int a = this.mapReduce(i -> ((int) i.getValue(Livre.NB_PAGES)) > 0,
				i -> new Livre((String) i.getValue(Livre.TITRE), (int) i.getValue(Livre.NB_PAGES) / 2),
				(acc, i) -> (acc + (int) i.getValue(Livre.NB_PAGES)), (acc, i) -> (acc + i), 0);
		System.out.println("Map reduce " + a);
	}

	
	 /**
     * Méthode qui démarre le composant client.
     * 
     * @throws ComponentStartException Si une erreur se produit lors du démarrage du composant.
     */
	@Override
	public synchronized void start() throws ComponentStartException {
		this.logMessage("starting client component.");
		super.start();
		try {
			if (!dsep.clientSideInitialised()) {
				this.dsep.initialiseClientSide(this);
			}
		} catch (ConnectionException e) {
			throw new ComponentStartException(e);
		}
	}

	  /**
     * Méthode d'exécution du composant client.
     * Elle exécute une tâche qui effectue des opérations sur le DHT de manière synchronisée.
     * 
     * @throws Exception Si une erreur se produit lors de l'exécution.
     */
	@Override
	public void execute() throws Exception {
		this.logMessage("executing client component.");

		this.runTask(new AbstractComponent.AbstractTask() {
			@Override
			public void run() {
				try {

					System.out.println(reflectionInboundPortURI);
					((ClientBCM) this.getTaskOwner()).getAndPrintContentData();
					((ClientBCM) this.getTaskOwner()).printMapReduceResult();
				} catch (Exception e) {
					e.printStackTrace();
				} finally {

				}
			}
		});
	}

	 /**
     * Méthode pour finaliser le composant client et effectuer le nettoyage.
     * 
     * @throws Exception Si une erreur se produit lors de la finalisation.
     */
	@Override
	public synchronized void finalise() throws Exception {
		this.logMessage("stopping client component.");
		this.printExecutionLogOnFile("client");

		this.dsep.cleanUpClientSide();

		super.finalise();
	}

}
