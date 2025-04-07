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

	@Override
	public void initialiseContent(NodeContentI content) throws Exception {
		this.getOwner().handleRequest(executorServiceIndex, owner -> {
			((DHTManagementI) owner).initialiseContent(content);
			return null;
		});

	}

	@Override
	public NodeStateI getCurrentState() throws Exception {
		return this.getOwner().handleRequest(executorServiceIndex, owner -> ((DHTManagementI) owner).getCurrentState());
	}

	@Override
	public NodeContentI suppressNode() throws Exception {
		return this.getOwner().handleRequest(executorServiceIndex, owner -> ((DHTManagementI) owner).suppressNode());
	}

	@Override
	public <CI extends ResultReceptionCI> void split(String computationURI, LoadPolicyI loadPolicy,
			EndPointI<CI> caller) throws Exception {
		this.getOwner().handleRequest(executorServiceIndex, owner -> {
			((DHTManagementI) owner).split(computationURI, loadPolicy, caller);
			return null;
		});

	}

	@Override
	public <CI extends ResultReceptionCI> void merge(String computationURI, LoadPolicyI loadPolicy,
			EndPointI<CI> caller) throws Exception {
		this.getOwner().handleRequest(executorServiceIndex, owner -> {
			((DHTManagementI) owner).merge(computationURI, loadPolicy, caller);
			return null;
		});
	}

	@Override
	public void computeChords(String computationURI, int numberOfChords) throws Exception {
		this.getOwner().handleRequest(executorServiceIndex, owner -> {
			((DHTManagementI) owner).computeChords(computationURI, numberOfChords);
			return null;
		});

	}

	@Override
	public SerializablePair<ContentNodeCompositeEndPointI<ContentAccessCI, ParallelMapReduceCI, DHTManagementCI>, Integer> getChordInfo(
			int offset) throws Exception {
		
		return this.getOwner().handleRequest(executorServiceIndex, owner -> ((DHTManagementI) owner).getChordInfo(offset));
	}

}
