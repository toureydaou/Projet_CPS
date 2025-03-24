package etape3.ports;


import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ResultReceptionCI;

//-----------------------------------------------------------------------------
/**
 * La classe <code>ContentAccessSyncInboundPort</code> implémente un port
 * entrant d'un composant serveur offrant les services de son interface offerte
 * <code>ContentAccessSyncCI</code> le serveur est donc contacté à travers son
 * port entrant.
 *
 * <p>
 * <strong>Description</strong>
 * </p>
 * 
 * <p>
 * Dans le cadre de ce projet les composants propriétaires de ce port sont les
 * noeuds.
 * </p>
 * 
 * @author Touré-Ydaou TEOURI
 * @author Awwal FAGBEHOURO
 */

public class AsynchronousContentAccessInboundPort extends AbstractInboundPort implements ContentAccessCI {

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
	public AsynchronousContentAccessInboundPort(ComponentI owner) throws Exception {
		super(ContentAccessCI.class, owner);

		// le propriétaire de ce port est un noeud jouant le role de serveur
		assert (owner instanceof ContentAccessI);
	}

	/**
	 * Crée et initialise un port entrant avec le composant propriétaire et une URI
	 * donnée.
	 * 
	 * @param owner Composant propriétaire du port.
	 * @throws Exception <i>to do</i>.
	 */
	public AsynchronousContentAccessInboundPort(String uri, ComponentI owner) throws Exception {
		super(uri, ContentAccessCI.class, owner);

		assert uri != null && (owner instanceof ContentAccessI);
	}

	@Override
	public <I extends ResultReceptionCI> void get(String computationURI, ContentKeyI key, EndPointI<I> caller)
			throws Exception {
		this.getOwner().runTask(owner -> {
			try {
				((ContentAccessI) owner).get(computationURI, key, caller);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		});
	}

	@Override
	public <I extends ResultReceptionCI> void put(String computationURI, ContentKeyI key, ContentDataI value,
			EndPointI<I> caller) throws Exception {
		this.getOwner().runTask(owner -> {
			try {
				((ContentAccessI) owner).put(computationURI, key, value, caller);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		});
		
	}

	@Override
	public <I extends ResultReceptionCI> void remove(String computationURI, ContentKeyI key, EndPointI<I> caller)
			throws Exception {
		this.getOwner().runTask(owner -> {
			try {
				((ContentAccessI) owner).remove(computationURI, key, caller);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		});
		
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
