package etape1;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.stream.Stream;

import fr.sorbonne_u.components.exceptions.ConnectionException;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;
import fr.sorbonne_u.cps.mapreduce.endpoints.POJOContentNodeCompositeEndPoint;
import fr.sorbonne_u.cps.mapreduce.utils.IntInterval;


/**
 * Classe {@code Node} représentant un nœud d'une DHT avec support de MapReduce.
 * Cette classe implémente les interfaces {@code ContentAccessSyncI} et {@code MapReduceSyncI}
 * pour gérer le stockage, la récupération et le traitement distribué des données.
 * 
 * <p><strong>Description</strong></p>
 *  
 *  <p>
 *	Les noeuds contiennent les données de la table et communiquent entre eux via des endpoints.
 *  Seul le premier noeud est relié à la façade. 
 * </p>
 * 
 * @author Touré-Ydaou TEOURI
 * @author Awwal FAGBEHOURO
 */

public class Node implements ContentAccessSyncI, MapReduceSyncI {
	
	// -------------------------------------------------------------------------
	// Constantes et variables
	// -------------------------------------------------------------------------
	
	/** Stockage local des données du nœud. */
    private HashMap<ContentKeyI, ContentDataI> content;
    /** Intervalle de clés gérées par ce nœud. */
    private IntInterval intervalle;
    /** Liste des URI des opérations en cours. */
    private ArrayList<String> uriPassages = new ArrayList<>();
    /** Liste des URI des opérations MapReduce en cours. */
    private ArrayList<String> uriPassagesMapReduce = new ArrayList<>();
    /** Mémoire temporaire pour le stockage des résultats intermédiaires de MapReduce. */
    private HashMap<String, Stream<ContentDataI>> memory = new HashMap<>();
    /** Endpoint pour la communication avec d'autres nœuds. */
    private POJOContentNodeCompositeEndPoint connexionSortante;
    
    // -------------------------------------------------------------------------
 	// Constructeurs
 	// -------------------------------------------------------------------------   
    
    /**
     * Initialise un nouveau nœud avec un intervalle de clés géré et des connexions entrantes et sortantes.
     * 
     * @param intervalle L'intervalle des clés gérées par ce nœud.
     * @param connexionEntrante Endpoint pour recevoir des requêtes.
     * @param connexionSortante Endpoint pour communiquer avec d'autres nœuds.
     * @throws ConnectionException si l'initialisation de la connexion entrante échoue.
     */
	public Node(IntInterval intervalle, POJOContentNodeCompositeEndPoint connexionEntrante,
			POJOContentNodeCompositeEndPoint connexionSortante) throws ConnectionException {
		this.content = new HashMap<ContentKeyI, ContentDataI>();
		this.intervalle = intervalle;
		this.connexionSortante = connexionSortante;
		connexionEntrante.initialiseServerSide(this);
	}
	
	// -------------------------------------------------------------------------
	// Méthodes
	// -------------------------------------------------------------------------
			

	@Override
	public <R extends Serializable> void mapSync(String computationURI, SelectorI selector, ProcessorI<R> processor)
			throws Exception {
		// Condition pour vérifier si le noeud est client d'un serveur préalablement initialisé
		if (!connexionSortante.clientSideInitialised()) {
			System.out.println("Serveur non initialisé " + intervalle.first());
			connexionSortante.initialiseClientSide(connexionSortante);
		}
		
		if (!uriPassagesMapReduce.contains(computationURI)) {
			uriPassagesMapReduce.add(computationURI);
			memory.put(computationURI,
					(Stream<ContentDataI>) content.values().stream().filter(selector).map(processor));
			this.connexionSortante.getMapReduceEndpoint().getClientSideReference().mapSync(computationURI, selector,
					processor);
		}

	}

	@Override
	public <A extends Serializable, R> A reduceSync(String computationURI, ReductorI<A, R> reductor,
			CombinatorI<A> combinator, A currentAcc) throws Exception {
		// Condition pour vérifier si le noeud est client d'un serveur préalablement initialisé
		if (!connexionSortante.clientSideInitialised()) {
			System.out.println("Serveur non initialisé " + intervalle.first());
			connexionSortante.initialiseClientSide(connexionSortante);
		}
		
		if (uriPassagesMapReduce.contains(computationURI)) {
			uriPassagesMapReduce.remove(computationURI);
			return combinator.apply(
					memory.get(computationURI).reduce(currentAcc, (u, d) -> reductor.apply(u, (R) d), combinator),
					this.connexionSortante.getMapReduceEndpoint().getClientSideReference().reduceSync(computationURI,
							reductor, combinator, currentAcc));
		} else {
			return currentAcc;
		}

	}

	@Override
	public void clearMapReduceComputation(String computationURI) throws Exception {
		// Condition pour vérifier si le noeud est client d'un serveur préalablement initialisé
		if (!connexionSortante.clientSideInitialised()) {
			System.out.println("Serveur non initialisé " + intervalle.first());
			connexionSortante.initialiseClientSide(connexionSortante);
		}
		
		if (memory.containsKey(computationURI)) {
			memory.remove(computationURI);
			this.connexionSortante.getMapReduceEndpoint().getClientSideReference()
					.clearMapReduceComputation(computationURI);
		}
	}

	@Override
	public ContentDataI getSync(String computationURI, ContentKeyI key) throws Exception {
		// Condition pour vérifier si le noeud est client d'un serveur préalablement initialisé
		if (!connexionSortante.clientSideInitialised()) {
			System.out.println("Serveur non initialisé " + intervalle.first());
			connexionSortante.initialiseClientSide(connexionSortante);
		}
		
		if (uriPassages.contains(computationURI)) {
			return null;
		} else {
			uriPassages.add(computationURI);
			int cle = ((EntierKey) key).getCle();
			if (intervalle.in(cle)) {
				return this.get(key);
			}
			return this.connexionSortante.getContentAccessEndpoint().getClientSideReference().getSync(computationURI,
					key);
		}
	}

	@Override
	public ContentDataI putSync(String computationURI, ContentKeyI key, ContentDataI value) throws Exception {
		// Condition pour vérifier si le noeud est client d'un serveur préalablement initialisé
		if (!connexionSortante.clientSideInitialised()) {
			System.out.println("Serveur non initialisé " + intervalle.first());
			connexionSortante.initialiseClientSide(connexionSortante);
		}
		
		if (uriPassages.contains(computationURI)) {
			return null;
		} else {
			uriPassages.add(computationURI);
			int cle = ((EntierKey) key).getCle();
			if (intervalle.in(cle)) {
				ContentDataI valuePrec = this.get(key);
				this.put(key, value);
				return valuePrec;
			}
			return this.connexionSortante.getContentAccessEndpoint().getClientSideReference().putSync(computationURI,
					key, value);
		}
	}

	@Override
	public ContentDataI removeSync(String computationURI, ContentKeyI key) throws Exception {
		// Condition pour vérifier si le noeud est client d'un serveur préalablement initialisé
		if (!connexionSortante.clientSideInitialised()) {
			System.out.println("Serveur non initialisé " + intervalle.first());
			connexionSortante.initialiseClientSide(connexionSortante);
		}
		
		if (uriPassages.contains(computationURI)) {
			return null;
		} else {
			uriPassages.add(computationURI);
			int n = ((EntierKey) key).getCle();
			if (intervalle.in(n)) {
				ContentDataI valuePrec = this.get(key);
				this.remove(key);
				return valuePrec;
			}
			return this.connexionSortante.getContentAccessEndpoint().getClientSideReference().removeSync(computationURI,
					key);
		}
	}

	@Override
	public void clearComputation(String computationURI) throws Exception {
		if (!connexionSortante.clientSideInitialised()) {
			System.out.println("Serveur non initialisé " + intervalle.first());
			connexionSortante.initialiseClientSide(connexionSortante);
		}
		if (uriPassages.contains(computationURI)) {
			uriPassages.remove(computationURI);
			this.connexionSortante.getContentAccessEndpoint().getClientSideReference().clearComputation(computationURI);
		}

	}

	/**
	 * Récupère les données associées à la clé spécifiée {@code key} dans la
	 * collection locale de données {@code content}. Étant donnée qu'il s'agit des
	 * réferences qui sont utilisées pour stocker nos données dans la DHT, on
	 * récupère donc dans la DHT la clé ayant la même valeur que {@code key} puis on
	 * récupère la valeur associé à la clé correspondante.
	 * 
	 * @param key La clé des données à récupérer.
	 * @return Les données associées à la clé {@code key} si elles existent, sinon
	 *         {@code null}.
	 */
	private ContentDataI get(ContentKeyI key) {
		for (ContentKeyI k : content.keySet()) {

			if (((EntierKey) k).getCle() == ((EntierKey) key).getCle()) {

				return content.get(k);
			}
		}
		return null;
	}

	/**
	 * Insère ou met à jour les données associées à la clé spécifiée {@code key}
	 * dans la collection locale de données {@code content}.
	 * 
	 * Si la clé existe déjà dans la collection, elle sera mise à jour avec la
	 * nouvelle valeur {@code data}. Sinon, la clé et la valeur seront ajoutées à la
	 * collection.
	 * 
	 * @param key  La clé sous laquelle les données doivent être insérées ou mises à
	 *             jour.
	 * @param data Les données à associer à la clé spécifiée.
	 */
	private void put(ContentKeyI key, ContentDataI data) {
		for (ContentKeyI k : content.keySet()) {
			if (((EntierKey) k).getCle() == ((EntierKey) key).getCle()) {
				content.put(k, data);
				break;
			}
		}
		content.put(key, data);
	}

	/**
	 * Supprime les données associées à la clé spécifiée {@code key} dans la
	 * collection locale de données {@code content}.
	 * 
	 * La méthode parcourt les clés dans la collection {@code content} et supprime
	 * la donnée associée à la clé spécifiée si une correspondance est trouvée.
	 * 
	 * @param key La clé des données à supprimer.
	 */
	private void remove(ContentKeyI key) {
		for (ContentKeyI k : content.keySet()) {
			if (((EntierKey) k).getCle() == ((EntierKey) key).getCle()) {
				content.remove(k);

				break;
			}
		}
	}

}