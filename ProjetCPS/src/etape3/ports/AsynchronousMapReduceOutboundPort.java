package etape3.ports;

import java.io.Serializable;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.components.interfaces.RequiredCI;
import fr.sorbonne_u.components.ports.AbstractOutboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceResultReceptionCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;

// TODO: Auto-generated Javadoc
//-----------------------------------------------------------------------------
/**
 * La classe <code>AsynchronousMapReduceOutboundPort</code> implémente un port
 * sortant pour les opérations MapReduce asynchrones. Ce port permet à un
 * composant client (comme la façade ou un nœud) d'invoquer les services d'un
 * composant serveur via l'interface {@link MapReduceCI}.
 *
 * <p>
 * <strong>Description</strong>
 * </p>
 * Ce port permet de lancer des opérations de type <code>map</code> et
 * <code>reduce</code> de manière asynchrone dans le cadre d'une exécution
 * MapReduce répartie sur un DHT.
 * 
 * <p>
 * <strong>Remarque :</strong> Les méthodes synchrones <code>mapSync</code> et
 * <code>reduceSync</code> ne sont pas supportées et doivent être utilisées
 * uniquement dans des ports synchrones.
 * </p>
 *
 * <p>
 * <strong>Utilisation</strong>
 * </p>
 * Ce port est utilisé par :
 * <ul>
 * <li>La façade (client MapReduce)</li>
 * <li>Les nœuds du DHT (dans les phases de réduction ou d'appel en
 * cascade)</li>
 * </ul>
 *
 * @author Touré-Ydaou TEOURI
 * @author Awwal FAGBEHOURO
 */

public class AsynchronousMapReduceOutboundPort extends AbstractOutboundPort implements MapReduceCI {

	// -------------------------------------------------------------------------
	// Constantes et variables
	// -------------------------------------------------------------------------

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = 1L;

	// -------------------------------------------------------------------------
	// Constructeurs
	// -------------------------------------------------------------------------

	/**
	 * Crée un port sortant asynchrone avec une URI spécifique et une interface
	 * requise.
	 *
	 * @param uri                  URI du port.
	 * @param implementedInterface Interface requise par le port.
	 * @param owner                Composant propriétaire du port.
	 * @throws Exception si une erreur survient lors de la création du port.
	 */
	public AsynchronousMapReduceOutboundPort(String uri, Class<? extends RequiredCI> implementedInterface,
			ComponentI owner) throws Exception {
		super(uri, implementedInterface, owner);
	}

	/**
	 * Crée un port sortant asynchrone pour l'interface {@link MapReduceCI}.
	 *
	 * @param implementedInterface the implemented interface
	 * @param owner Composant propriétaire du port.
	 * @throws Exception si une erreur survient lors de la création du port.
	 */
	public AsynchronousMapReduceOutboundPort(Class<? extends RequiredCI> implementedInterface, ComponentI owner)
			throws Exception {
		super(implementedInterface, owner);
	}

	/**
	 * Crée un port sortant asynchrone pour l'interface {@link MapReduceCI} avec une
	 * URI donnée.
	 *
	 * @param owner Composant propriétaire du port.
	 * @throws Exception si une erreur survient lors de la création du port.
	 */
	public AsynchronousMapReduceOutboundPort(ComponentI owner) throws Exception {
		super(MapReduceCI.class, owner);

		// le propriétaire de ce port est un noeud ou la facade jouant le role de client
		assert owner != null;
	}

	/**
	 * Crée et initialise un port sortant avec le composant propriétaire et une URI
	 * donnée.
	 *
	 * @param uri the uri
	 * @param owner Composant propriétaire du port.
	 * @throws Exception <i>to do</i>.
	 */
	public AsynchronousMapReduceOutboundPort(String uri, ComponentI owner) throws Exception {
		super(uri, MapReduceCI.class, owner);

		assert uri != null && owner != null;
	}


	/**
	 * Reduce.
	 *
	 * @param <A> the generic type
	 * @param <R> the generic type
	 * @param <I> the generic type
	 * @param computationURI the computation URI
	 * @param reductor the reductor
	 * @param combinator the combinator
	 * @param identityAcc the identity acc
	 * @param currentAcc the current acc
	 * @param callerNode the caller node
	 * @throws Exception the exception
	 */
	@Override
	public <A extends Serializable, R, I extends MapReduceResultReceptionCI> void reduce(String computationURI,
			ReductorI<A, R> reductor, CombinatorI<A> combinator, A identityAcc, A currentAcc, EndPointI<I> callerNode)
			throws Exception {
		((MapReduceCI) this.getConnector()).reduce(computationURI, reductor, combinator, identityAcc, currentAcc,
				callerNode);

	}

	
	@Override
	public <R extends Serializable> void mapSync(String computationURI, SelectorI selector, ProcessorI<R> processor)
			throws Exception {
		// TODO Auto-generated method stub

	}

	
	@Override
	public <A extends Serializable, R> A reduceSync(String computationURI, ReductorI<A, R> reductor,
			CombinatorI<A> combinator, A currentAcc) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	
	/**
	 * Clear map reduce computation.
	 *
	 * @param computationURI the computation URI
	 * @throws Exception the exception
	 */
	@Override
	public void clearMapReduceComputation(String computationURI) throws Exception {
		((MapReduceCI) this.getConnector()).clearMapReduceComputation(computationURI);
	}

	/**
	 * Map.
	 *
	 * @param <R> the generic type
	 * @param computationURI the computation URI
	 * @param selector the selector
	 * @param processor the processor
	 * @throws Exception the exception
	 */
	@Override
	public <R extends Serializable> void map(String computationURI, SelectorI selector, ProcessorI<R> processor)
			throws Exception {
		((MapReduceCI) this.getConnector()).map(computationURI, selector, processor);

	}

}
