package etape2.composants;

import java.io.Serializable;
import java.util.HashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;

import etape2.endpoints.CompositeMapContentSyncEndpoint;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.annotations.OfferedInterfaces;
import fr.sorbonne_u.components.annotations.RequiredInterfaces;
import fr.sorbonne_u.components.exceptions.ComponentShutdownException;
import fr.sorbonne_u.components.exceptions.ComponentStartException;
import fr.sorbonne_u.components.exceptions.ConnectionException;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;
import fr.sorbonne_u.cps.mapreduce.utils.IntInterval;

//-----------------------------------------------------------------------------
/**
 * La classe <code>NodeBCM</code> représente un nœud d'une table de hachage
 * répartie (DHT). Elle offre des services de stockage et de récupération de
 * données, ainsi que des fonctionnalités de calcul distribué basées sur
 * MapReduce.
 * 
 * <p>
 * <strong>Description</strong>
 * </p>
 * 
 * <p>
 * Chaque instance de <code>NodeBCM</code> gère un intervalle spécifique de clés
 * et communique avec d'autres noeuds via des ports entrants et sortants.
 * </p>
 * 
 * @author Touré-Ydaou TEOURI
 * @author Awwal FAGBEHOURO
 */

@OfferedInterfaces(offered = { ContentAccessSyncCI.class, MapReduceSyncCI.class })
@RequiredInterfaces(required = { ContentAccessSyncCI.class, MapReduceSyncCI.class })
public class NodeBCM extends AbstractComponent {

	// Stocke les données associées aux clés de la DHT
	protected HashMap<ContentKeyI, ContentDataI> content;

	// Définition de l'intervalle de clés géré par ce nœud
	protected IntInterval intervalle;

	// Listes des URI des computations MapReduce et de stockage déjà traitées
	protected CopyOnWriteArrayList<String> uriPassCont = new CopyOnWriteArrayList<>();
	protected CopyOnWriteArrayList<String> uriPassMap = new CopyOnWriteArrayList<>();
	

	// Mémoire temporaire pour stocker les résultats intermédiaires des computations
	// MapReduce
	private HashMap<String, Stream<ContentDataI>> memory = new HashMap<>();

	// Ports pour la communication avec d'autres noeuds
	protected CompositeMapContentSyncEndpoint cmceInbound; // Port entrant (serveur)
	protected CompositeMapContentSyncEndpoint cmceOutbound; // Port sortant (client)

	private static final int SCHEDULABLE_THREADS = 2;
	private static final int THREADS_NUMBER = 0;

	/**
	 * Constructeur d'un nœud de la DHT.
	 * 
	 * @param uri          URI du composant
	 * @param cmceInbound  Port entrant du composant
	 * @param cmceOutbound Port sortant du composant
	 * @param intervalle   Intervalle de clés gérées par ce noeud
	 * @throws ConnectionException en cas d'erreur de connexion
	 */
	protected NodeBCM(String uri, CompositeMapContentSyncEndpoint cmceInbound, CompositeMapContentSyncEndpoint cmceOutbound,
			IntInterval intervalle) throws ConnectionException {
		super(uri, THREADS_NUMBER, SCHEDULABLE_THREADS);
		this.content = new HashMap<>();
		this.intervalle = intervalle;
		this.cmceInbound = cmceInbound;
		this.cmceOutbound = cmceOutbound;
		cmceInbound.initialiseServerSide(this);
	}

	/**
	 * Applique la fonction de traitement {@code processor} sur les entrées de la
	 * DHT sélectionnées par {@code selector}, et stocke les résultats en mémoire.
	 * 
	 * @param computationURI Identifiant unique de la computation MapReduce en
	 *                       cours.
	 * @param selector       Fonction qui filtre les données d'entrée avant
	 *                       traitement.
	 * @param processor      Fonction qui transforme les données sélectionnées.
	 * @throws Exception .
	 */

	@SuppressWarnings("unchecked")
	public <R extends Serializable> void mapSync(String computationURI, SelectorI selector, ProcessorI<R> processor)
			throws Exception {
		System.out.println("Reception de la requête 'MAP REDUCE' (MAP) sur le noeud " + this.intervalle.first() + " - "
				+ this.intervalle.last() + ", identifiant de la requete : " + computationURI);
		if (!uriPassMap.contains(computationURI)) {
			uriPassMap.add(computationURI);
			memory.put(computationURI,
					(Stream<ContentDataI>) content.values().stream().filter(selector).map(processor));
			this.cmceOutbound.getMapReduceEndPoint().getClientSideReference().mapSync(computationURI, selector,
					processor);
		} else {
			return;
		}

	}

	/**
	 * Réduit les résultats d'une computation MapReduce en utilisant les fonctions
	 * {@code reductor} et {@code combinator}.
	 * 
	 * @param <A>            Le type de l'accumulateur utilisé pour la réduction.
	 * 
	 * @param <R>            Le type des résultats de la computation map qui vont
	 *                       être réduits.
	 * @param computationURI URI de la computation, utilisé pour distinguer les
	 *                       différents traitements parallèles effectués sur la DHT.
	 * @param reductor       Fonction qui applique une opération de réduction entre
	 *                       un accumulateur courant et un résultat de la map.
	 * @param combinator     Fonction qui combine deux valeurs d'accumulateur pour
	 *                       obtenir un nouvel accumulateur.
	 * @param currentAcc     La valeur courante de l'accumulateur.
	 * @return Le nouvel accumulateur calculé après la réduction synchrone.
	 * @throws Exception
	 */

	@SuppressWarnings("unchecked")
	public <A extends Serializable, R> A reduceSync(String computationURI, ReductorI<A, R> reductor,
			CombinatorI<A> combinator, A currentAcc) throws Exception {
		System.out.println("Reception de la requête 'MAP REDUCE' (REDUCE) sur le noeud " + this.intervalle.first()
				+ " - " + this.intervalle.last() + ", identifiant de la requete : " + computationURI);
		if (uriPassMap.contains(computationURI)) {
			uriPassMap.remove(computationURI);
			return combinator.apply(
					memory.get(computationURI).reduce(currentAcc, (u, d) -> reductor.apply(u, (R) d), combinator),
					this.cmceOutbound.getMapReduceEndPoint().getClientSideReference().reduceSync(computationURI,
							reductor, combinator, currentAcc));
		} else {
			return currentAcc;
		}

	}

	/**
	 * Supprime les données liées à une computation MapReduce, identifiée par son
	 * URI {@code computationURI}. Cette méthode efface les résultats précédemment
	 * stockés dans la mémoire locale pour l'URI spécifié et envoie une demande pour
	 * nettoyer également les données associées sur les autres noeuds via le port de
	 * sortie.
	 * 
	 * @param computationURI L'URI de la computation MapReduce dont les données
	 *                       doivent être supprimées.
	 * @throws Exception
	 */

	public void clearMapReduceComputation(String computationURI) throws Exception {
		System.out.println("Nettoyage des opérations du map reduce sur le noeud " + this.intervalle.first() + " - "
				+ this.intervalle.last() + ", identifiant de la requete : " + computationURI);
		if (memory.containsKey(computationURI)) {
			memory.remove(computationURI);
			this.cmceOutbound.getMapReduceEndPoint().getClientSideReference().clearMapReduceComputation(computationURI);
		}

	}

	/**
	 * Récupère les données associées à une clé spécifiée {@code key} dans le cadre
	 * d'une computation identifiée par l'URI {@code computationURI}. Cette méthode
	 * vérifie si la computation a déjà été traitée, et si c'est le cas, elle
	 * renvoie les données locales. Sinon, elle délègue la requête à un autre noeud
	 * via le port de sortie.
	 * 
	 * @param computationURI L'URI de la computation pour laquelle les données sont
	 *                       demandées.
	 * @param key            La clé associée aux données à récupérer.
	 * @return Les données associées à la clé {@code key}, ou {@code null} si la
	 *         computation a déjà été traitée localement.
	 * @throws Exception
	 */
	public ContentDataI getSync(String computationURI, ContentKeyI key) throws Exception {
		System.out.println("Reception de la requête 'GET' sur le noeud " + this.intervalle.first() + " - "
				+ this.intervalle.last() + ", identifiant de la requete : " + computationURI);
		if (uriPassCont.contains(computationURI)) {
			return null;
		} else {
			uriPassCont.add(computationURI);
			int cle = key.hashCode();
			if (intervalle.in(cle)) {
				return content.get(key);
			}
			return this.cmceOutbound.getContentAccessEndPoint().getClientSideReference().getSync(computationURI, key);
		}
	}

	/**
	 * Insère ou met à jour les données associées à une clé spécifiée {@code key}
	 * pour une computation identifiée par l'URI {@code computationURI}. Si la clé
	 * est dans l'intervalle du noeud, les données sont stockées localement. Sinon,
	 * la requête est envoyée à un autre nœud via le port de sortie.
	 * 
	 * @param computationURI L'URI de la computation pour laquelle les données
	 *                       doivent être insérées ou mises à jour.
	 * @param key            La clé associée aux données à insérer ou mettre à jour.
	 * @param value          Les nouvelles données à insérer ou à utiliser pour
	 *                       mettre à jour les anciennes données associées à la clé.
	 * @return Les anciennes données associées à la clé {@code key} avant la mise à
	 *         jour, ou {@code null} si la computation a déjà été traitée
	 *         localement.
	 * @throws Exception
	 */

	public ContentDataI putSync(String computationURI, ContentKeyI key, ContentDataI value) throws Exception {
		System.out.println("Reception de la requête 'PUT' sur le noeud " + this.intervalle.first() + " - "
				+ this.intervalle.last() + ", identifiant de la requete : " + computationURI);
		if (uriPassCont.contains(computationURI)) {
			return null;
		} else {
			uriPassCont.add(computationURI);
			int cle = key.hashCode();
			if (intervalle.in(cle)) {
				ContentDataI valuePrec = content.get(key);
				content.put(key, value);
				return valuePrec;
			}
			return this.cmceOutbound.getContentAccessEndPoint().getClientSideReference().putSync(computationURI, key,
					value);

		}
	}

	/**
	 * Supprime les données associées à une clé spécifiée {@code key} pour une
	 * computation identifiée par {@code computationURI}. Si la clé est dans
	 * l'intervalle du noeud, les données sont supprimées localement. Sinon, la
	 * requête est envoyée à un autre noeud via le port de sortie.
	 * 
	 * @param computationURI L'URI de la computation pour laquelle les données
	 *                       doivent être supprimées.
	 * @param key            La clé des données à supprimer.
	 * @return Les anciennes données associées à la clé {@code key} avant la
	 *         suppression, ou {@code null} si la computation a déjà été traitée
	 *         localement.
	 * @throws Exception
	 */

	public ContentDataI removeSync(String computationURI, ContentKeyI key) throws Exception {
		System.out.println("Reception de la requête 'REMOVE' sur le noeud " + this.intervalle.first() + " - "
				+ this.intervalle.last() + ", identifiant de la requete : " + computationURI);
		if (uriPassCont.contains(computationURI)) {
			return null;
		} else {
			uriPassCont.add(computationURI);
			int n = key.hashCode();
			if (intervalle.in(n)) {
				ContentDataI valuePrec = content.remove(key);
				return valuePrec;
			}
			return this.cmceOutbound.getContentAccessEndPoint().getClientSideReference().removeSync(computationURI,
					key);
		}

	}

	/**
	 * Efface les données liées à une computation spécifique identifiée par
	 * {@code computationURI}.
	 * 
	 * <p>
	 * Si le URI de la computation est présent dans la liste {@code uriPassCont}, il
	 * est retiré de cette liste. La méthode appelle ensuite le serveur distant pour
	 * nettoyer les données associées à cette computation.
	 * </p>
	 * 
	 * @param computationURI L'URI de la computation dont les données doivent être
	 *                       effacées.
	 * @throws Exception
	 */

	public void clearComputation(String computationURI) throws Exception {
		System.out.println("Nettoyage sur le noeud " + this.intervalle.first() + " - " + this.intervalle.last()
				+ ", identifiant de la requete : " + computationURI);
		if (uriPassCont.contains(computationURI)) {
			uriPassCont.remove(computationURI);
			this.cmceOutbound.getContentAccessEndPoint().getClientSideReference().clearComputation(computationURI);
		}
	}


	/**
	 * Démarre le composant ClientBCM.
	 * 
	 * @throws ComponentStartException Si une erreur se produit lors du démarrage du
	 *                                 composant.
	 */
	@Override
	public void start() throws ComponentStartException {
		this.logMessage("starting node component.");
		super.start();
		try {
			if (!this.cmceOutbound.clientSideInitialised()) {
				this.cmceOutbound.initialiseClientSide(this);
			}
		} catch (ConnectionException e) {
			throw new ComponentStartException(e);
		}
	}

	/**
	 * Finalise et arrête le composant ClientBCM.
	 * 
	 * @throws Exception Si une erreur se produit lors de l'arrêt du composant.
	 */
	@Override
	public void finalise() throws Exception {
		this.logMessage("stopping node component.");
		this.printExecutionLogOnFile("node");
		this.cmceOutbound.cleanUpClientSide();
		super.finalise();
	}

	/**
	 * Effectue un arrêt propre du composant ClientBCM.
	 * 
	 * @throws ComponentShutdownException Si une erreur se produit lors de l'arrêt.
	 */
	@Override
	public void shutdown() throws ComponentShutdownException {
		try {
			this.cmceInbound.cleanUpServerSide();
		} catch (Exception e) {
			throw new ComponentShutdownException(e);
		}
		super.shutdown();
	}

	/**
	 * Force un arrêt immédiat du composant ClientBCM.
	 * 
	 * @throws ComponentShutdownException Si une erreur se produit lors de l'arrêt
	 *                                    immédiat.
	 */
	@Override
	public void shutdownNow() throws ComponentShutdownException {
		try {
			this.cmceInbound.cleanUpServerSide();
		} catch (Exception e) {
			throw new ComponentShutdownException(e);
		}
		super.shutdownNow();
	}

}
