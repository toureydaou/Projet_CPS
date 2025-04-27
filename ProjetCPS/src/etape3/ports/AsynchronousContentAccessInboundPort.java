package etape3.ports;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ResultReceptionCI;

//-----------------------------------------------------------------------------
/**
 * La classe {@code AsynchronousContentAccessInboundPort} implémente un port
 * entrant pour un composant serveur offrant les services de l'interface
 * {@code ContentAccessCI}.
 * <p>
 * Ce port permet aux clients d'effectuer des opérations d'accès au contenu de
 * manière asynchrone en envoyant leurs requêtes au travers d'un
 * {@code EndPointI}.
 * </p>
 * 
 * <p>
 * Le propriétaire de ce port est un composant jouant le rôle d'un nœud dans un
 * système de table de hachage distribuée (DHT) intégrant des fonctionnalités de
 * type MapReduce.
 * </p>
 * 
 * <p>
 * Les opérations {@code get}, {@code put} et {@code remove} sont exécutées de
 * manière asynchrone via l'exécuteur spécifié, tandis que les méthodes
 * synchrones ({@code getSync}, {@code putSync}, {@code removeSync}) ne sont pas
 * implémentées ici (retourne {@code null}).
 * </p>
 * 
 * @author Touré-Ydaou TEOURI
 * @author Awwal FAGBEHOURO
 */
public class AsynchronousContentAccessInboundPort extends AbstractInboundPort implements ContentAccessCI {

	// -------------------------------------------------------------------------
	// Constantes et variables
	// -------------------------------------------------------------------------

	private static final long serialVersionUID = 1L;
	protected final int executorIndex;

	// -------------------------------------------------------------------------
	// Constructeurs
	// -------------------------------------------------------------------------

	/**
	 * Crée et initialise un port entrant pour un composant propriétaire donné.
	 *
	 * @param executorIndex indice du service d'exécution pour traiter les requêtes
	 * @param owner         composant propriétaire du port
	 * @throws Exception si une erreur survient lors de l'initialisation
	 */
	public AsynchronousContentAccessInboundPort(int executorIndex, ComponentI owner) throws Exception {
		super(ContentAccessCI.class, owner);

		// le propriétaire de ce port est un noeud jouant le role de serveur
		assert (owner instanceof ContentAccessI);

		assert owner.validExecutorServiceIndex(executorIndex);

		this.executorIndex = executorIndex;
	}

	/**
	 * Crée et initialise un port entrant avec une URI spécifique et un composant
	 * propriétaire.
	 *
	 * @param uri           URI du port
	 * @param executorIndex indice du service d'exécution pour traiter les requêtes
	 * @param owner         composant propriétaire du port
	 * @throws Exception si une erreur survient lors de l'initialisation
	 */
	public AsynchronousContentAccessInboundPort(String uri, int executorIndex, ComponentI owner) throws Exception {
		super(uri, ContentAccessCI.class, owner);

		assert uri != null && (owner instanceof ContentAccessI);
		assert owner.validExecutorServiceIndex(executorIndex);

		this.executorIndex = executorIndex;
	}

	/**
	 * Lance une requête asynchrone pour obtenir la valeur associée à une clé dans
	 * la DHT.
	 *
	 * @param computationURI URI de la computation MapReduce
	 * @param key            clé du contenu recherché
	 * @param caller         endpoint pour réceptionner le résultat
	 * @throws Exception si une erreur survient pendant le traitement
	 */
	@Override
	public <I extends ResultReceptionCI> void get(String computationURI, ContentKeyI key, EndPointI<I> caller)
			throws Exception {

		this.getOwner().runTask(executorIndex, owner -> {
			try {
				((ContentAccessI) owner).get(computationURI, key, caller);
			} catch (Exception e) {
				e.printStackTrace();
			}
		});

	}

	/**
	 * Lance une requête asynchrone pour insérer une paire clé-valeur dans la DHT.
	 *
	 * @param computationURI URI de la computation MapReduce
	 * @param key            clé du contenu à insérer
	 * @param value          valeur du contenu à insérer
	 * @param caller         endpoint pour réceptionner la confirmation
	 * @throws Exception si une erreur survient pendant le traitement
	 */
	@Override
	public <I extends ResultReceptionCI> void put(String computationURI, ContentKeyI key, ContentDataI value,
			EndPointI<I> caller) throws Exception {
		this.getOwner().runTask(executorIndex, owner -> {
			try {
				((ContentAccessI) owner).put(computationURI, key, value, caller);
			} catch (Exception e) {
				e.printStackTrace();
			}
		});

	}

	/**
	 * Lance une requête asynchrone pour retirer une entrée (clé-valeur) de la DHT.
	 *
	 * @param computationURI URI de la computation MapReduce
	 * @param key            clé du contenu à supprimer
	 * @param caller         endpoint pour réceptionner la confirmation
	 * @throws Exception si une erreur survient pendant le traitement
	 */
	@Override
	public <I extends ResultReceptionCI> void remove(String computationURI, ContentKeyI key, EndPointI<I> caller)
			throws Exception {
		this.getOwner().runTask(executorIndex, owner -> {
			try {
				((ContentAccessI) owner).remove(computationURI, key, caller);
			} catch (Exception e) {
				e.printStackTrace();
			}
		});

	}

	@Override
	public ContentDataI getSync(String computationURI, ContentKeyI key) throws Exception {

		return null;
	}

	@Override
	public ContentDataI putSync(String computationURI, ContentKeyI key, ContentDataI value) throws Exception {

		return null;
	}

	@Override
	public ContentDataI removeSync(String computationURI, ContentKeyI key) throws Exception {

		return null;
	}

	/**
	 * Lance une requête asynchrone pour supprimer tous les contenus associés à une
	 * computation donnée.
	 *
	 * @param computationURI URI de la computation MapReduce
	 * @throws Exception si une erreur survient pendant le traitement
	 */
	@Override
	public void clearComputation(String computationURI) throws Exception {
		this.getOwner().runTask(executorIndex, owner -> {
			try {
				((ContentAccessI) owner).clearComputation(computationURI);
			} catch (Exception e) {
				e.printStackTrace();
			}
		});

	}

}
