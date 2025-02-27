package etape2.endpoints;

import etape2.connecteurs.ContentAccessSyncConnector;
import etape2.ports.ContentAccessSyncInboundPort;
import etape2.ports.ContentAccessSyncOutboundPort;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.AbstractPort;
import fr.sorbonne_u.components.endpoints.BCMEndPoint;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncCI;
import fr.sorbonne_u.exceptions.ImplementationInvariantException;
import fr.sorbonne_u.exceptions.InvariantException;
import fr.sorbonne_u.exceptions.PostconditionException;
import fr.sorbonne_u.exceptions.PreconditionException;

//-----------------------------------------------------------------------------
/**
 * La classe <code>ContentAccessEndPoint</code> implémente un endpoint
 * permettant de connecteur un composant client et serveur. En connectant le
 * port sortant <code>ContentAccessSyncOutboundPort</code> du client et le port
 * entrant <code>ContentAccessSyncInboundPort</code> du serveur grace au
 * connecteur <code>ContentAccessSyncConnector</code>.
 *
 * <strong>Description</strong>
 * </p>
 * 
 * <p>
 * Cette classe représente les endpoints utilisés pour connecter les noeuds
 * entre eux, mais aussi la facade au premier noeud. Il sera contenu dans un
 * <code>CompositeMapContentEndpoint</code>.
 * </p>
 * 
 * @author Touré-Ydaou TEOURI
 * @author Awwal FAGBEHOURO
 */

public class ContentAccessEndPoint extends BCMEndPoint<ContentAccessSyncCI> {

	// -------------------------------------------------------------------------
	// Constants and variables
	// -------------------------------------------------------------------------

	private static final long serialVersionUID = 1L;

	protected static boolean implementationInvariants(ContentAccessEndPoint instance) {
		assert instance != null : new PreconditionException("instance != null");

		boolean ret = true;
		return ret;
	}

	protected static boolean invariants(ContentAccessEndPoint instance) {
		assert instance != null : new PreconditionException("instance != null");

		boolean ret = true;
		return ret;
	}

	/**
	 * crée un endpoint BCM avec URI donnée.
	 *
	 * 
	 * @param inboundPortURI URI du port entrant auquel cet endpoint se connecte.
	 */
	public ContentAccessEndPoint(String inboundPortURI) {
		super(ContentAccessSyncCI.class, ContentAccessSyncCI.class, inboundPortURI);
	}

	/**
	 * crée un endpoint BCM.
	 *
	 */
	public ContentAccessEndPoint() {
		super(ContentAccessSyncCI.class, ContentAccessSyncCI.class);
	}

	/**
	 * crée, publie et retourne le port entrant sur le composant serveur {@code c}
	 * avec l'URI du port entrant
	 *
	 * @param c              composant qui sera propriétaire du port entrant.
	 * @param inboundPortURI URI du port entrant à créer.
	 * @return le port entrant créé destiné à être publié.
	 * @throws Exception <i>to do</i>.
	 */
	@Override
	protected AbstractInboundPort makeInboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
		// Preconditions checking
		assert c != null : new PreconditionException("c != null");
		assert inboundPortURI != null && !inboundPortURI.isEmpty()
				: new PreconditionException("inboundPortURI != null && !inboundPortURI.isEmpty()");

		ContentAccessSyncInboundPort p = new ContentAccessSyncInboundPort(this.inboundPortURI, c);
		p.publishPort();

		// Postconditions checking
		assert p != null && p.isPublished() : new PostconditionException("return != null && return.isPublished()");
		assert ((AbstractPort) p).getPortURI().equals(inboundPortURI)
				: new PostconditionException("((AbstractPort)return).getPortURI().equals(inboundPortURI)");
		assert getServerSideInterface().isAssignableFrom(p.getClass())
				: new PostconditionException("getOfferedComponentInterface()." + "isAssignableFrom(return.getClass())");
		// Invariant checking
		assert ContentAccessEndPoint.implementationInvariants(this)
				: new ImplementationInvariantException("implementationInvariants(this)");
		assert ContentAccessEndPoint.invariants(this) : new InvariantException("invariants(this)");

		return p;
	}

	/**
	 * crée, publie, connecte et retourne le port sortant du composant client
	 * {@code c}. on the client side component only, create, publish, connect and
	 * return the outbound port requiring the component interface {@code CI} on the
	 * client side component {@code c}.
	 * 
	 *
	 * @param c              composant qui sera propriétaire du port entrant.
	 * @param inboundPortURI URI du port entrant auquel le port sortant sera
	 *                       connecté.
	 * @return le port sortant qui sera connecté et publié.
	 * @throws Exception <i>to do</i>.
	 */
	@Override
	protected ContentAccessSyncCI makeOutboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
		// Preconditions checking
		assert c != null : new PreconditionException("c != null");

		ContentAccessSyncOutboundPort p = new ContentAccessSyncOutboundPort(c);
		p.publishPort();
		c.doPortConnection(p.getPortURI(), this.inboundPortURI, ContentAccessSyncConnector.class.getCanonicalName());

		// Postconditions checking
		assert p != null && p.isPublished() && p.connected()
				: new PostconditionException("return != null && return.isPublished() && " + "return.connected()");
		assert ((AbstractPort) p).getServerPortURI().equals(getInboundPortURI()) : new PostconditionException(
				"((AbstractPort)return).getServerPortURI()." + "equals(getInboundPortURI())");
		assert getClientSideInterface().isAssignableFrom(p.getClass())
				: new PostconditionException("getImplementedInterface().isAssignableFrom(" + "return.getClass())");

		// Invariant checking
		assert implementationInvariants(this) : new ImplementationInvariantException("implementationInvariants(this)");
		assert invariants(this) : new InvariantException("invariants(this)");

		return p;
	}

}
