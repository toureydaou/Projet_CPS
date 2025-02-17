package etape2.ports;

import etape2.composants.NodeBCM;
import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;

public class ContentAccessSyncInboundPort extends AbstractInboundPort implements ContentAccessSyncCI {

	private static final long serialVersionUID = 1L;

	public ContentAccessSyncInboundPort(ComponentI owner)
			throws Exception {
		super(ContentAccessSyncCI.class, owner);
		
		// le propriétaire de ce port est un noeud jouant le role de serveur
		assert	(owner instanceof NodeBCM);
	}

	public ContentAccessSyncInboundPort(String uri, ComponentI owner)
			throws Exception {
		super(uri, ContentAccessSyncCI.class, owner);
		
		assert uri != null  &&	(owner instanceof NodeBCM);
	}

	@Override
	public ContentDataI getSync(String computationURI, ContentKeyI key) throws Exception {
		return this.getOwner().handleRequest(
				owner -> ((NodeBCM) owner).getSync(computationURI, key));
	}

	@Override
	public ContentDataI putSync(String computationURI, ContentKeyI key, ContentDataI value) throws Exception {
		return this.getOwner().handleRequest(owner -> ((NodeBCM) owner).putSync(computationURI, key, value));
	}

	@Override
	public ContentDataI removeSync(String computationURI, ContentKeyI key) throws Exception {
		return this.getOwner().handleRequest(owner -> ((NodeBCM) owner).removeSync(computationURI, key));
	}

	@Override
	public void clearComputation(String computationURI) throws Exception {
		this.getOwner().handleRequest(
				owner -> {
						((NodeBCM) owner).clearComputation(computationURI);
						return null;
					});
	}


}
