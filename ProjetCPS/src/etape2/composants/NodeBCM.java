package etape2.composants;


import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.annotations.OfferedInterfaces;
import fr.sorbonne_u.components.annotations.RequiredInterfaces;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncCI;

@OfferedInterfaces(offered = {ContentAccessSyncCI.class, MapReduceSyncCI.class})
@RequiredInterfaces(required = {ContentAccessSyncCI.class, MapReduceSyncCI.class})
public class NodeBCM extends AbstractComponent{

	protected NodeBCM(String reflectionInboundPortURI, int nbThreads, int nbSchedulableThreads) {
		super(reflectionInboundPortURI, nbThreads, nbSchedulableThreads);

	}

	public ContentDataI getSync(String computationURI, ContentKeyI key)
			throws Exception{
		return null;
	}
	public ContentDataI putSync(
			String computationURI, ContentKeyI key, ContentDataI value
			) throws Exception{
		return null;
	}
	public ContentDataI removeSync(String computationURI, ContentKeyI key)
			throws Exception{
				return null;
			}
	public void
	clearComputation(String computationURI) throws Exception{
		return;
	}	

}
