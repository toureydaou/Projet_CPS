package etape3.ports;

import java.io.Serializable;


import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceResultReceptionCI;
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

public class AsynchronousMapReduceInboundPort extends AbstractInboundPort implements MapReduceCI {

	// -------------------------------------------------------------------------
	// Constantes et variables
	// -------------------------------------------------------------------------

	private static final long serialVersionUID = 1L;

	// -------------------------------------------------------------------------
	// Constructeurs
	// -------------------------------------------------------------------------

	/**
	 * Crée et initialise le port entrant avec le composant propriétaire.
	 * 
	 * @param owner Composant propriétaire du port.
	 * @throws Exception <i>to do</i>.
	 */
	public AsynchronousMapReduceInboundPort(ComponentI owner) throws Exception {
		super(MapReduceCI.class, owner);

		// le propriétaire de ce port est un noeud jouant le role de serveur
		assert (owner instanceof MapReduceI);
	}

	/**
	 * Crée et initialise un port entrant avec le composant propriétaire et une URI
	 * donnée.
	 * 
	 * @param owner Composant propriétaire du port.
	 * @throws Exception <i>to do</i>.
	 */
	public AsynchronousMapReduceInboundPort(String uri, ComponentI owner) throws Exception {
		super(uri, MapReduceCI.class, owner);

		assert uri != null && (owner instanceof MapReduceI);
	}

	@Override
	public <R extends Serializable, I extends MapReduceResultReceptionCI> void map(String computationURI,
			SelectorI selector, ProcessorI<R> processor) throws Exception {
		this.getOwner().runTask(owner -> {
			try {
				((MapReduceI) owner).map(computationURI, selector, processor);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		});
	}

	@Override
	public <A extends Serializable, R, I extends MapReduceResultReceptionCI> void reduce(String computationURI,
			ReductorI<A, R> reductor, CombinatorI<A> combinator, A identityAcc, A currentAcc, EndPointI<I> callerNode)
			throws Exception {
		this.getOwner().runTask(owner -> {
			try {
				((MapReduceI) owner).reduce(computationURI, reductor, combinator, identityAcc, currentAcc, callerNode);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		});
		
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

	@Override
	public void clearMapReduceComputation(String computationURI) throws Exception {
		// TODO Auto-generated method stub
		
	}

	
}
