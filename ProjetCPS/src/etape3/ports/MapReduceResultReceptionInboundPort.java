package etape3.ports;

import java.io.Serializable;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceResultReceptionCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceResultReceptionI;

// TODO: Auto-generated Javadoc
/**
 * The Class MapReduceResultReceptionInboundPort.
 */
public class MapReduceResultReceptionInboundPort extends AbstractInboundPort implements MapReduceResultReceptionCI {

	// -------------------------------------------------------------------------
	// Constantes et variables
	// -------------------------------------------------------------------------

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = 1L;
	
	/** The executor index. */
	protected final int executorIndex;

	// -------------------------------------------------------------------------
	// Constructeurs
	// -------------------------------------------------------------------------

	
	/**
	 * Instancie un nouveau port entrant de réception des résultats de MapReduce.
	 *
	 * @param executorIndex l'indice de l'exécuteur
	 * @param owner le propriétaire
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
	 * @param uri the uri
	 * @param executorIndex the executor index
	 * @param owner Composant propriétaire du port.
	 * @throws Exception <i>to do</i>.
	 */
	public MapReduceResultReceptionInboundPort(String uri, int executorIndex, ComponentI owner) throws Exception {
		super(uri, MapReduceResultReceptionCI.class, owner);

		assert uri != null && (owner instanceof MapReduceResultReceptionI);
		assert owner.validExecutorServiceIndex(executorIndex);

		this.executorIndex = executorIndex;
	}


	/**
	 * Accepte le résultat d'une opération MapReduce.
	 * 
	 * Cette méthode permet de recevoir un résultat intermédiaire ou final d'un 
	 * calcul MapReduce effectué sur un autre nœud. 
	 * 
	 * @param computationURI L'URI de la computation MapReduce.
	 * @param emitterId L'ID de l'émetteur du résultat.
	 * @param acc L'accumulateur contenant le résultat du calcul.
	 * @throws Exception Si une erreur se produit lors du traitement.
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
