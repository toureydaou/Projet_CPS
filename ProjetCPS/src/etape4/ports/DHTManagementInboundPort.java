package etape4.ports;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ResultReceptionCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.endpoints.ContentNodeCompositeEndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.management.DHTManagementCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.management.DHTManagementI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.management.LoadPolicyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ParallelMapReduceCI;
import fr.sorbonne_u.cps.mapreduce.utils.SerializablePair;

//-----------------------------------------------------------------------------
/**
* La classe {@code DHTManagementInboundPort} implémente un port
* entrant pour un composant serveur offrant les services de l'interface
* {@code DHTManagementCI}.
* <p>
* Ce port permet aux clients d'effectuer des opérations de gestion de composants de
* manière synchrone en envoyant leurs requêtes au travers d'un
* {@code EndPointI}.
* </p>
* 
* <p>
* Le propriétaire de ce port est un composant jouant le rôle d'un nœud dans un
* système de table de hachage distribuée (DHT) intégrant des fonctionnalités de
* type DHTManagement.
* </p>
* 
* @see DHTManagementCI
* @see AbstractInboundPort
* 
* @author Touré-Ydaou TEOURI
* @author Awwal FAGBEHOURO
*/
public class DHTManagementInboundPort extends AbstractInboundPort implements DHTManagementCI {

	// -------------------------------------------------------------------------
	// Constantes et variables
	// -------------------------------------------------------------------------

	private static final long serialVersionUID = 1L;
	protected final int executorServiceIndex;

	// -------------------------------------------------------------------------
	// Constructeurs
	// -------------------------------------------------------------------------

	/**
	 * Crée et initialise le port entrant avec le composant propriétaire.
	 * 
	 * @param owner Composant propriétaire du port.
	 * @throws Exception <i>to do</i>.
	 */
	public DHTManagementInboundPort(int executorServiceIndex, ComponentI owner) throws Exception {
		super(DHTManagementCI.class, owner);

		// le propriétaire de ce port est un noeud jouant le role de serveur
		assert (owner instanceof DHTManagementI);

		assert owner.validExecutorServiceIndex(executorServiceIndex);

		this.executorServiceIndex = executorServiceIndex;
	}

	/**
	 * Crée et initialise un port entrant avec le composant propriétaire et une URI
	 * donnée.
	 * 
	 * @param owner Composant propriétaire du port.
	 * @throws Exception <i>to do</i>.
	 */
	public DHTManagementInboundPort(String uri, int executorServiceIndex, ComponentI owner) throws Exception {
		super(uri, DHTManagementCI.class, owner);

		assert uri != null && (owner instanceof DHTManagementI);

		assert owner.validExecutorServiceIndex(executorServiceIndex);

		this.executorServiceIndex = executorServiceIndex;
	}
	
	// -------------------------------------------------------------------------
	// Méthodes
	// -------------------------------------------------------------------------

	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.management.DHTManagementCI#initialiseContent(fr.sorbonne_u.cps.dht_mapreduce.interfaces.management.DHTManagementI.NodeContentI)
	 */
	@Override
	public void initialiseContent(NodeContentI content) throws Exception {
		this.getOwner().handleRequest(executorServiceIndex, owner -> {
			((DHTManagementI) owner).initialiseContent(content);
			return null;
		});

	}

	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.management.DHTManagementCI#getCurrentState()
	 */
	@Override
	public NodeStateI getCurrentState() throws Exception {
		return this.getOwner().handleRequest(executorServiceIndex, owner -> ((DHTManagementI) owner).getCurrentState());
	}

	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.management.DHTManagementCI#suppressNode()
	 */
	@Override
	public NodeContentI suppressNode() throws Exception {
		return this.getOwner().handleRequest(executorServiceIndex, owner -> ((DHTManagementI) owner).suppressNode());
	}

	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.management.DHTManagementCI#split(java.lang.String, fr.sorbonne_u.cps.dht_mapreduce.interfaces.management.LoadPolicyI, fr.sorbonne_u.components.endpoints.EndPointI)
	 */
	@Override
	public <CI extends ResultReceptionCI> void split(String computationURI, LoadPolicyI loadPolicy,
			EndPointI<CI> caller) throws Exception {
		this.getOwner().handleRequest(executorServiceIndex, owner -> {
			((DHTManagementI) owner).split(computationURI, loadPolicy, caller);
			return null;
		});

	}

	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.management.DHTManagementCI#merge(java.lang.String, fr.sorbonne_u.cps.dht_mapreduce.interfaces.management.LoadPolicyI, fr.sorbonne_u.components.endpoints.EndPointI)
	 */
	@Override
	public <CI extends ResultReceptionCI> void merge(String computationURI, LoadPolicyI loadPolicy,
			EndPointI<CI> caller) throws Exception {
		this.getOwner().handleRequest(executorServiceIndex, owner -> {
			((DHTManagementI) owner).merge(computationURI, loadPolicy, caller);
			return null;
		});
	}

	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.management.DHTManagementCI#computeChords(java.lang.String, int)
	 */
	@Override
	public void computeChords(String computationURI, int numberOfChords) throws Exception {
		this.getOwner().handleRequest(executorServiceIndex, owner -> {
			((DHTManagementI) owner).computeChords(computationURI, numberOfChords);
			return null;
		});

	}

	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.management.DHTManagementCI#getChordInfo(int)
	 */
	@Override
	public SerializablePair<ContentNodeCompositeEndPointI<ContentAccessCI, ParallelMapReduceCI, DHTManagementCI>, Integer> getChordInfo(
			int offset) throws Exception {
		
		return this.getOwner().handleRequest(executorServiceIndex, owner -> ((DHTManagementI) owner).getChordInfo(offset));
	}

}
