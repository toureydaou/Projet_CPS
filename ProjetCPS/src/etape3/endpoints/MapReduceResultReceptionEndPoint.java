package etape3.endpoints;

import etape3.connecteurs.MapReduceResultReceptionConnector;
import etape3.ports.MapReduceResultReceptionInboundPort;
import etape3.ports.MapReduceResultReceptionOutboundPort;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.AbstractPort;
import fr.sorbonne_u.components.endpoints.BCMEndPoint;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceResultReceptionCI;
import fr.sorbonne_u.exceptions.ImplementationInvariantException;
import fr.sorbonne_u.exceptions.InvariantException;
import fr.sorbonne_u.exceptions.PostconditionException;
import fr.sorbonne_u.exceptions.PreconditionException;

/**
 * La classe {@code MapReduceResultReceptionEndPoint} implémente un point
 * d'extrémité BCM spécifiquement destiné à la réception des résultats d'une
 * exécution MapReduce.
 * 
 * <p>
 * Elle permet de créer un point d'extrémité permettant de recevoir de manière
 * asynchrone les résultats d'un calcul MapReduce en provenance d'un autre
 * composant.
 * </p>
 * 
 * <p>
 * Cet end-point est utilisé pour la réception des résultats après l'exécution
 * d'un calcul MapReduce dans un système distribué, avec gestion asynchrone des
 * tâches.
 * </p>
 * 
 * @see etape3.connecteurs.MapReduceResultReceptionConnector
 * @see etape3.ports.MapReduceResultReceptionInboundPort
 * @see etape3.ports.MapReduceResultReceptionOutboundPort
 * 
 * @author Touré-Ydaou TEOURI
 * @author Awwal FAGBEHOURO
 */
public class MapReduceResultReceptionEndPoint extends BCMEndPoint<MapReduceResultReceptionCI> {
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
	protected static boolean implementationInvariants(MapReduceResultReceptionEndPoint instance) {
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
	protected static boolean invariants(MapReduceResultReceptionEndPoint instance) {
		assert instance != null : new PreconditionException("instance != null");

		boolean ret = true;
		return ret;
	}

	/**
	 * crée un endpoint BCM avec URI donnée.
	 *
	 * @param inboundPortURI       URI du port entrant auquel cet endpoint se
	 *                             connecte.
	 * @param executorServiceIndex the executor service index
	 */
	public MapReduceResultReceptionEndPoint(String inboundPortURI, int executorServiceIndex) {
		super(MapReduceResultReceptionCI.class, MapReduceResultReceptionCI.class, inboundPortURI);
		this.executorServiceIndex = executorServiceIndex;
	}

	/**
	 * crée un endpoint BCM.
	 *
	 */
	public MapReduceResultReceptionEndPoint() {
		super(MapReduceResultReceptionCI.class, MapReduceResultReceptionCI.class);
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

		MapReduceResultReceptionInboundPort p = new MapReduceResultReceptionInboundPort(this.inboundPortURI,
				this.executorServiceIndex, c);
		p.publishPort();

		// Postconditions checking
		assert p != null && p.isPublished() : new PostconditionException("return != null && return.isPublished()");
		assert ((AbstractPort) p).getPortURI().equals(inboundPortURI)
				: new PostconditionException("((AbstractPort)return).getPortURI().equals(inboundPortURI)");
		assert getServerSideInterface().isAssignableFrom(p.getClass())
				: new PostconditionException("getOfferedComponentInterface()." + "isAssignableFrom(return.getClass())");
		// Invariant checking
		assert MapReduceResultReceptionEndPoint.implementationInvariants(this)
				: new ImplementationInvariantException("implementationInvariants(this)");
		assert MapReduceResultReceptionEndPoint.invariants(this) : new InvariantException("invariants(this)");

		return p;
	}

	/**
	 * 
	 * @see fr.sorbonne_u.components.endpoints.BCMEndPoint#makeOutboundPort(fr.sorbonne_u.components.AbstractComponent,
	 *      java.lang.String)
	 */
	@Override
	protected MapReduceResultReceptionCI makeOutboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
		// Preconditions checking
		assert c != null : new PreconditionException("c != null");

		MapReduceResultReceptionOutboundPort p = new MapReduceResultReceptionOutboundPort(c);
		p.publishPort();
		c.doPortConnection(p.getPortURI(), this.inboundPortURI,
				MapReduceResultReceptionConnector.class.getCanonicalName());

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
