package etape3.endpoints;

import etape3.connecteurs.ResultReceptionConnector;
import etape3.ports.ResultReceptionInboundPort;
import etape3.ports.ResultReceptionOutboundPort;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.AbstractPort;
import fr.sorbonne_u.components.endpoints.BCMEndPoint;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ResultReceptionCI;
import fr.sorbonne_u.exceptions.ImplementationInvariantException;
import fr.sorbonne_u.exceptions.InvariantException;
import fr.sorbonne_u.exceptions.PostconditionException;
import fr.sorbonne_u.exceptions.PreconditionException;

/**
 * La classe {@code ResultReceptionEndPoint} implémente un point d'extrémité BCM
 * destiné à la réception des résultats dans un système distribué.
 * 
 * <p>
 * Cette classe permet de recevoir de manière asynchrone les résultats d'un
 * traitement distribué dans un environnement basé sur BCM (Broadcast
 * Communication Model).
 * </p>
 * 
 * <p>
 * Cet end-point est conçu pour faciliter la gestion des résultats dans un
 * environnement distribué en permettant une communication entre composants via
 * des ports BCM.
 * </p>
 * 
 * @see etape3.connecteurs.ResultReceptionConnector
 * @see etape3.ports.ResultReceptionInboundPort
 * @see etape3.ports.ResultReceptionOutboundPort
 * 
 * @author Touré-Ydaou TEOURI
 * @author Awwal FAGBEHOURO
 */
public class ResultReceptionEndPoint extends BCMEndPoint<ResultReceptionCI> {
	// -------------------------------------------------------------------------
	// Constants and variables
	// -------------------------------------------------------------------------

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = 1L;

	/** The executor service index. */
	protected int executorServiceIndex;

	/**
	 * Implementation invariants.
	 *
	 * @param instance the instance
	 * @return true, if successful
	 */
	protected static boolean implementationInvariants(ResultReceptionEndPoint instance) {
		assert instance != null : new PreconditionException("instance != null");

		boolean ret = true;
		return ret;
	}

	/**
	 * Invariants.
	 *
	 * @param instance the instance
	 * @return true, if successful
	 */
	protected static boolean invariants(ResultReceptionEndPoint instance) {
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
	public ResultReceptionEndPoint(String inboundPortURI) {
		super(ResultReceptionCI.class, ResultReceptionCI.class, inboundPortURI);
	}

	/**
	 * crée un endpoint BCM.
	 *
	 */
	public ResultReceptionEndPoint() {
		super(ResultReceptionCI.class, ResultReceptionCI.class);
	}

	/**
	 * 
	 * @see fr.sorbonne_u.components.endpoints.BCMEndPoint#makeInboundPort(fr.sorbonne_u.components.AbstractComponent,
	 *      java.lang.String)
	 */
	@Override
	protected AbstractInboundPort makeInboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
		// Preconditions checking
		assert c != null : new PreconditionException("c != null");
		assert inboundPortURI != null && !inboundPortURI.isEmpty()
				: new PreconditionException("inboundPortURI != null && !inboundPortURI.isEmpty()");

		ResultReceptionInboundPort p = new ResultReceptionInboundPort(this.inboundPortURI, this.executorServiceIndex,
				c);
		p.publishPort();

		// Postconditions checking
		assert p != null && p.isPublished() : new PostconditionException("return != null && return.isPublished()");
		assert ((AbstractPort) p).getPortURI().equals(inboundPortURI)
				: new PostconditionException("((AbstractPort)return).getPortURI().equals(inboundPortURI)");
		assert getServerSideInterface().isAssignableFrom(p.getClass())
				: new PostconditionException("getOfferedComponentInterface()." + "isAssignableFrom(return.getClass())");
		// Invariant checking
		assert ResultReceptionEndPoint.implementationInvariants(this)
				: new ImplementationInvariantException("implementationInvariants(this)");
		assert ResultReceptionEndPoint.invariants(this) : new InvariantException("invariants(this)");

		return p;
	}

	/**
	 * 
	 * @see fr.sorbonne_u.components.endpoints.BCMEndPoint#makeOutboundPort(fr.sorbonne_u.components.AbstractComponent,
	 *      java.lang.String)
	 */
	@Override
	protected ResultReceptionCI makeOutboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
		// Preconditions checking
		assert c != null : new PreconditionException("c != null");

		ResultReceptionOutboundPort p = new ResultReceptionOutboundPort(c);
		p.publishPort();
		c.doPortConnection(p.getPortURI(), this.inboundPortURI, ResultReceptionConnector.class.getCanonicalName());

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

	/**
	 * Sets the executor index.
	 *
	 * @param executorIndex the new executor index
	 */
	public void setExecutorIndex(int executorIndex) {
		this.executorServiceIndex = executorIndex;
	}

}
