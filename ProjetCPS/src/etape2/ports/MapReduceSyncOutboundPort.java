package etape2.ports;

import java.io.Serializable;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.interfaces.RequiredCI;
import fr.sorbonne_u.components.ports.AbstractOutboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;

//-----------------------------------------------------------------------------
/**
 * La classe <code>MapReduceSyncInboundPort</code> implémente un port entrant
 * d'un composant serveur offrant les services de son interface offerte
 * <code>MapReduceSyncCI</code> le serveur est donc contacté à travers son port
 * entrant.
 *
 * <p>
 * <strong>Description</strong>
 * </p>
 * 
 * <p>
 * Dans le cadre de ce projet les composants propriétaires de ce port sont la
 * facade et les noeuds.
 * </p>
 * 
 * @author Touré-Ydaou TEOURI
 * @author Awwal FAGBEHOURO
 */

public class MapReduceSyncOutboundPort extends AbstractOutboundPort implements MapReduceSyncCI {

	// -------------------------------------------------------------------------
	// Constantes et variables
	// -------------------------------------------------------------------------

	private static final long serialVersionUID = 1L;

	// -------------------------------------------------------------------------
	// Constructeurs
	// -------------------------------------------------------------------------

	/**
	 * Crée et initialise le port sortant avec le composant propriétaire.
	 * 
	 * @param owner Composant propriétaire du port.
	 * @throws Exception <i>to do</i>.
	 */
	public MapReduceSyncOutboundPort(ComponentI owner) throws Exception {
		super(MapReduceSyncCI.class, owner);

		// le propriétaire de ce port est un noeud ou la facade jouant le role de client
		assert owner != null;
	}

	/**
	 * Crée et initialise un port sortant avec le composant propriétaire et une URI
	 * donnée.
	 * 
	 * @param owner Composant propriétaire du port.
	 * @throws Exception <i>to do</i>.
	 */
	public MapReduceSyncOutboundPort(String uri, ComponentI owner) throws Exception {
		super(uri, MapReduceSyncCI.class, owner);

		assert uri != null && owner != null;
	}
	
	

	public MapReduceSyncOutboundPort(Class<? extends RequiredCI> implementedInterface, ComponentI owner) throws Exception {
		super(implementedInterface, owner);	
	}
	
	
	public MapReduceSyncOutboundPort(String uri, Class<? extends RequiredCI> implementedInterface, ComponentI owner) throws Exception {
		super(uri, implementedInterface, owner);	
	}
	

	/**
	 * Invoque la méthode <code>mapSync</code> du connecteur
	 * <code>MapReduceSyncConnector</code> pour déclencher un map sur les données de
	 * la table de hachage.
	 * 
	 * @param <R>            type des résultats de la computation map.
	 * @param computationURI URI de la computation, utilisé pour distinguer les
	 *                       computations parallèles sur la même DHT.
	 * @param selector       une fonction booléenne qui sélectionne les entrées de
	 *                       la DHT à traiter.
	 * @param processor      une fonction implémentant le traitement à appliquer par
	 *                       la map.
	 * @throws Exception <i>à compléter</i>.
	 */
	@Override
	public <R extends Serializable> void mapSync(String computationURI, SelectorI selector, ProcessorI<R> processor)
			throws Exception {
		((MapReduceSyncCI) this.getConnector()).mapSync(computationURI, selector, processor);
	}

	/**
	 * Invoque la méthode <code>reduceSync</code> du connecteur
	 * <code>MapReduceSyncConnector</code> pour déclencher un reduce sur les données
	 * de la table de hachage.
	 * 
	 * @param <R>        type du résultat de l'opération du map.
	 * @param <A>        type de l'accumulateur de l'opération du reduce.
	 * 
	 * @param reductor   fonction {@code A} x {@code R -> A} accumulant un résultat
	 *                   de la map.
	 * @param combinator fonction {@code A} x {@code A -> A} combinant deux
	 *                   accumulateurs.
	 * @param initialAcc valeur initiale de l'accumulateur.
	 * @return valeur finale de l'accumulateur après toutes les reduces.
	 * @throws Exception <i>to do</i>.
	 */
	@Override
	public <A extends Serializable, R> A reduceSync(String computationURI, ReductorI<A, R> reductor,
			CombinatorI<A> combinator, A currentAcc) throws Exception {
		return ((MapReduceSyncCI) this.getConnector()).reduceSync(computationURI, reductor, combinator, currentAcc);
	}

	/**
	 * Invoque la méthode <code>clearMapReduceComputation</code> du connecteur
	 * <code>MapReduceSyncConnector</code> pour nettoyer toutes les données
	 * restantes du dernier map reduce effectué.
	 * 
	 * @param <R>        type du résultat de l'opération du map.
	 * @param <A>        type de l'accumulateur de l'opération du reduce.
	 * 
	 * @param reductor   fonction {@code A} x {@code R -> A} accumulant un résultat
	 *                   de la map.
	 * @param combinator fonction {@code A} x {@code A -> A} combinant deux
	 *                   accumulateurs.
	 * @param initialAcc valeur initiale de l'accumulateur.
	 * @return valeur finale de l'accumulateur après toutes les reduces.
	 * @throws Exception <i>to do</i>.
	 */
	@Override
	public void clearMapReduceComputation(String computationURI) throws Exception {
		((MapReduceSyncCI) this.getConnector()).clearMapReduceComputation(computationURI);
	}

}
