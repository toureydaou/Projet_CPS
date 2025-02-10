package etape2.connecteurs;

import etape2.composants.NodeBCM;
import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractOutboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;

public class ContentAccessSyncOutboundPort extends AbstractOutboundPort implements ContentAccessSyncCI {
	
	private static final long serialVersionUID = 1L;

	public ContentAccessSyncOutboundPort(ComponentI owner)
			throws Exception {
		super(ContentAccessSyncCI.class, owner);
		
		// le propriétaire de ce port est un noeud ou la facade jouant le role de client
		assert	owner instanceof NodeBCM ;
	}

	public ContentAccessSyncOutboundPort(String uri, ComponentI owner)
			throws Exception {
		super(uri, ContentAccessSyncCI.class, owner);
		
		assert uri != null  &&	owner instanceof NodeBCM ;
	}

	
	@Override
	public ContentDataI getSync(String computationURI, ContentKeyI key) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ContentDataI putSync(String computationURI, ContentKeyI key, ContentDataI value) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ContentDataI removeSync(String computationURI, ContentKeyI key) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void clearComputation(String computationURI) throws Exception {
		// TODO Auto-generated method stub

	}

}
