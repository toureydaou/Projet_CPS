package etape3.ports;

import java.io.Serializable;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceResultReceptionCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceResultReceptionI;

/**
 * La classe <code>MapReduceResultReceptionInboundPort</code> représente un port
 * entrant pour recevoir les résultats intermédiaires ou finaux d'une
 * computation MapReduce.
 *
 * <p>
 * Ce port invoque la méthode {@code acceptResult} du composant propriétaire
 * (qui doit implémenter {@link MapReduceResultReceptionI}).
 * </p>
 * 
 * @author Touré-Ydaou TEOURI
 * @author Awwal FAGBEHOURO
 */
public class MapReduceResultReceptionInboundPort extends AbstractInboundPort implements MapReduceResultReceptionCI {

	// -------------------------------------------------------------------------
	// Constantes et variables
	// -------------------------------------------------------------------------

	private static final long serialVersionUID = 1L;

	protected final int executorIndex;

	// -------------------------------------------------------------------------
	// Constructeurs
	// -------------------------------------------------------------------------

	/**
	 * Instancie un nouveau port entrant de réception des résultats de MapReduce.
	 *
	 * @param executorIndex l'indice de l'exécuteur
	 * @param owner         le propriétaire
	 * @throws Exception l'exception
	 */
	public MapReduceResultReceptionInboundPort(int executorIndex, ComponentI owner) throws Exception {
		super(MapReduceResultReceptionCI.class, owner);

		// le propriétaire de ce port est un noeud jouant le role de serveur
		assert (owner instanceof MapReduceResultReceptionI);

		assert owner.validExecutorServiceIndex(executorIndex);

		this.executorIndex = executorIndex;
	}

	/**
	 * Crée et initialise un port entrant avec le composant propriétaire et une URI
	 * donnée.
	 *
	 * @param uri           the uri
	 * @param executorIndex the executor index
	 * @param owner         Composant propriétaire du port.
	 * @throws Exception <i>to do</i>.
	 */
	public MapReduceResultReceptionInboundPort(String uri, int executorIndex, ComponentI owner) throws Exception {
		super(uri, MapReduceResultReceptionCI.class, owner);

		assert uri != null && (owner instanceof MapReduceResultReceptionI);
		assert owner.validExecutorServiceIndex(executorIndex);

		this.executorIndex = executorIndex;
	}

	/**
	 * 
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceResultReceptionCI#acceptResult(java.lang.String,
	 *      java.lang.String, java.io.Serializable)
	 */
	@Override
	public void acceptResult(String computationURI, String emitterId, Serializable acc) throws Exception {
		this.getOwner().runTask(executorIndex, owner -> {
			try {
				((MapReduceResultReceptionI) owner).acceptResult(computationURI, emitterId, acc);
			} catch (Exception e) {
				e.printStackTrace();
			}
		});
	}

}
