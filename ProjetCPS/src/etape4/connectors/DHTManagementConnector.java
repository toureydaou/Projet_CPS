package etape4.connectors;

import fr.sorbonne_u.components.connectors.AbstractConnector;
import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ResultReceptionCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.endpoints.ContentNodeCompositeEndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.management.DHTManagementCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.management.LoadPolicyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ParallelMapReduceCI;
import fr.sorbonne_u.cps.mapreduce.utils.SerializablePair;

/**
 * Classe {@code DHTManagementConnector} reliant deux composants via l'interface DHTManagementCI.
 * 
 * Elle délègue les appels au composant offrant, permettant ainsi d'exécuter des opérations 
 * de DHTManagement dans la DHT.
 */
public class DHTManagementConnector extends AbstractConnector implements DHTManagementCI {

	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.management.DHTManagementCI#initialiseContent(fr.sorbonne_u.cps.dht_mapreduce.interfaces.management.DHTManagementI.NodeContentI)
	 */
	@Override
	public void initialiseContent(NodeContentI content) throws Exception {
		((DHTManagementCI) this.offering).initialiseContent(content);

	}

	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.management.DHTManagementCI#getCurrentState()
	 */
	@Override
	public NodeStateI getCurrentState() throws Exception {
		return ((DHTManagementCI) this.offering).getCurrentState();

	}

	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.management.DHTManagementCI#suppressNode()
	 */
	@Override
	public NodeContentI suppressNode() throws Exception {
		return ((DHTManagementCI) this.offering).suppressNode();
	}

	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.management.DHTManagementCI#split(java.lang.String, fr.sorbonne_u.cps.dht_mapreduce.interfaces.management.LoadPolicyI, fr.sorbonne_u.components.endpoints.EndPointI)
	 */
	@Override
	public <CI extends ResultReceptionCI> void split(String computationURI, LoadPolicyI loadPolicy,
			EndPointI<CI> caller) throws Exception {
		((DHTManagementCI) this.offering).split(computationURI, loadPolicy, caller);

	}

	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.management.DHTManagementCI#merge(java.lang.String, fr.sorbonne_u.cps.dht_mapreduce.interfaces.management.LoadPolicyI, fr.sorbonne_u.components.endpoints.EndPointI)
	 */
	@Override
	public <CI extends ResultReceptionCI> void merge(String computationURI, LoadPolicyI loadPolicy,
			EndPointI<CI> caller) throws Exception {
		((DHTManagementCI) this.offering).merge(computationURI, loadPolicy, caller);

	}

	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.management.DHTManagementCI#computeChords(java.lang.String, int)
	 */
	@Override
	public void computeChords(String computationURI, int numberOfChords) throws Exception {
		((DHTManagementCI) this.offering).computeChords(computationURI, numberOfChords);

	}

	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.management.DHTManagementCI#getChordInfo(int)
	 */
	@Override
	public SerializablePair<ContentNodeCompositeEndPointI<ContentAccessCI, ParallelMapReduceCI, DHTManagementCI>, Integer> getChordInfo(
			int offset) throws Exception {
		return ((DHTManagementCI) this.offering).getChordInfo(offset);
	}

}
