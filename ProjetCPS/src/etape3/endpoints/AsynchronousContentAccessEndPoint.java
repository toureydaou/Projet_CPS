package etape3.endpoints;

import etape3.connecteurs.ContentAccessConnector;
import etape3.ports.AsynchronousContentAccessInboundPort;
import etape3.ports.AsynchronousContentAccessOutboundPort;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.AbstractPort;
import fr.sorbonne_u.components.endpoints.BCMEndPoint;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessCI;
import fr.sorbonne_u.exceptions.ImplementationInvariantException;
import fr.sorbonne_u.exceptions.InvariantException;
import fr.sorbonne_u.exceptions.PostconditionException;
import fr.sorbonne_u.exceptions.PreconditionException;

// TODO: Auto-generated Javadoc
/**
 * La classe <code>AsynchronousContentAccessEndPoint</code> représente un point
 * d'accès pour les services de <code>ContentAccessCI</code>, permettant la
 * communication asynchrone entre les composants. Elle gère les connexions et la
 * création des ports entrants et sortants nécessaires pour interagir avec les
 * composants serveur et client.
 *
 * <p>
 * Un <code>AsynchronousContentAccessEndPoint</code> permet à un composant de se
 * connecter à d'autres composants via des ports sortants et entrants, en
 * fournissant une interface pour l'accès au service de contenu.
 * </p>
 * 
 * <p>
 * Ce point d'accès est utilisé dans un contexte où la communication asynchrone
 * est nécessaire, permettant ainsi de gérer les échanges de données de manière
 * non bloquante.
 * </p>
 * 
 * @author Touré-Ydaou TEOURI
 * @author Awwal FAGBEHOURO
 */

public class AsynchronousContentAccessEndPoint extends BCMEndPoint<ContentAccessCI> {
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
	protected static boolean implementationInvariants(AsynchronousContentAccessEndPoint instance) {
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
	protected static boolean invariants(AsynchronousContentAccessEndPoint instance) {
		assert instance != null : new PreconditionException("instance != null");

		boolean ret = true;
		return ret;
	}

	/**
	 * Crée un point d'accès BCM avec une URI donnée.
	 * 
	 * @param inboundPortURI       URI du port entrant auquel cet endpoint se
	 *                             connecte.
	 * @param executorServiceIndex Indice du service exécutant.
	 */
	public AsynchronousContentAccessEndPoint(String inboundPortURI, int executorServiceIndex) {
		super(ContentAccessCI.class, ContentAccessCI.class, inboundPortURI);
		this.executorServiceIndex = executorServiceIndex;
	}

	/**
	 * Crée un point d'accès BCM sans URI spécifique.
	 */
	public AsynchronousContentAccessEndPoint() {
		super(ContentAccessCI.class, ContentAccessCI.class);

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

		AsynchronousContentAccessInboundPort p = new AsynchronousContentAccessInboundPort(this.inboundPortURI,
				this.executorServiceIndex, c);
		p.publishPort();

		// Postconditions checking
		assert p != null && p.isPublished() : new PostconditionException("return != null && return.isPublished()");
		assert ((AbstractPort) p).getPortURI().equals(inboundPortURI)
				: new PostconditionException("((AbstractPort)return).getPortURI().equals(inboundPortURI)");
		assert getServerSideInterface().isAssignableFrom(p.getClass())
				: new PostconditionException("getOfferedComponentInterface()." + "isAssignableFrom(return.getClass())");
		// Invariant checking
		assert AsynchronousContentAccessEndPoint.implementationInvariants(this)
				: new ImplementationInvariantException("implementationInvariants(this)");
		assert AsynchronousContentAccessEndPoint.invariants(this) : new InvariantException("invariants(this)");

		return p;
	}

	/**
	 * 
	 * @see fr.sorbonne_u.components.endpoints.BCMEndPoint#makeOutboundPort(fr.sorbonne_u.components.AbstractComponent,
	 *      java.lang.String)
	 */
	@Override
	protected ContentAccessCI makeOutboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
		// Preconditions checking
		assert c != null : new PreconditionException("c != null");

		AsynchronousContentAccessOutboundPort p = new AsynchronousContentAccessOutboundPort(c);
		p.publishPort();
		c.doPortConnection(p.getPortURI(), this.inboundPortURI, ContentAccessConnector.class.getCanonicalName());

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
	 * Définit l'indice du service exécutant.
	 * 
	 * @param executorIndex L'indice du service exécutant.
	 */
	public void setExecutorIndex(int executorIndex) {
		this.executorServiceIndex = executorIndex;
	}

}
