package etape3.endpoints;

import etape3.connecteurs.MapReduceConnector;
import etape3.ports.AsynchronousMapReduceInboundPort;
import etape3.ports.AsynchronousMapReduceOutboundPort;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.AbstractPort;
import fr.sorbonne_u.components.endpoints.BCMEndPoint;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceCI;
import fr.sorbonne_u.exceptions.ImplementationInvariantException;
import fr.sorbonne_u.exceptions.InvariantException;
import fr.sorbonne_u.exceptions.PostconditionException;
import fr.sorbonne_u.exceptions.PreconditionException;

public class AsynchronousMapReduceEndPoint extends BCMEndPoint<MapReduceCI> {

	// -------------------------------------------------------------------------
	// Constants and variables
	// -------------------------------------------------------------------------

	private static final long serialVersionUID = 1L;

	protected static boolean implementationInvariants(AsynchronousMapReduceEndPoint instance) {
		assert instance != null : new PreconditionException("instance != null");

		boolean ret = true;
		return ret;
	}

	protected static boolean invariants(AsynchronousMapReduceEndPoint instance) {
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
	public AsynchronousMapReduceEndPoint(String inboundPortURI) {
		super(MapReduceCI.class, MapReduceCI.class, inboundPortURI);
	}

	/**
	 * crée un endpoint BCM.
	 *
	 */
	public AsynchronousMapReduceEndPoint() {
		super(MapReduceCI.class, MapReduceCI.class);
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

		AsynchronousMapReduceInboundPort p = new AsynchronousMapReduceInboundPort(this.inboundPortURI, c);
		p.publishPort();

		// Postconditions checking
		assert p != null && p.isPublished() : new PostconditionException("return != null && return.isPublished()");
		assert ((AbstractPort) p).getPortURI().equals(inboundPortURI)
				: new PostconditionException("((AbstractPort)return).getPortURI().equals(inboundPortURI)");
		assert getServerSideInterface().isAssignableFrom(p.getClass())
				: new PostconditionException("getOfferedComponentInterface()." + "isAssignableFrom(return.getClass())");
		// Invariant checking
		assert AsynchronousMapReduceEndPoint.implementationInvariants(this)
				: new ImplementationInvariantException("implementationInvariants(this)");
		assert AsynchronousMapReduceEndPoint.invariants(this) : new InvariantException("invariants(this)");

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
	protected MapReduceCI makeOutboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
		// Preconditions checking
		assert c != null : new PreconditionException("c != null");

		AsynchronousMapReduceOutboundPort p = new AsynchronousMapReduceOutboundPort(c);
		p.publishPort();
		c.doPortConnection(p.getPortURI(), this.inboundPortURI, MapReduceConnector.class.getCanonicalName());

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
