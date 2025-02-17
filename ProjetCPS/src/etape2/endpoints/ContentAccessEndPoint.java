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

public class ContentAccessEndPoint extends BCMEndPoint<ContentAccessSyncCI> {
	
	private static final long serialVersionUID = 1L;

	protected static boolean	implementationInvariants(
			ContentAccessEndPoint instance
			)
	{
		assert instance != null : new PreconditionException("instance != null");

		boolean ret = true;
		return ret;
	}
	
	protected static boolean	invariants(ContentAccessEndPoint instance)
	{
		assert instance != null : new PreconditionException("instance != null");

		boolean ret = true;
		return ret;
	}
	
	public				ContentAccessEndPoint(
			String inboundPortURI,
			String outboundPortURI)
	{
		super(ContentAccessSyncCI.class, ContentAccessSyncCI.class,
				inboundPortURI, outboundPortURI);
	}
	
	public				ContentAccessEndPoint()
	{
		super(ContentAccessSyncCI.class, ContentAccessSyncCI.class);
	}
	
	@Override
	protected AbstractInboundPort	makeInboundPort(
		AbstractComponent c,
		String inboundPortURI
		) throws Exception
	{
		// Preconditions checking
		assert	c != null : new PreconditionException("c != null");
		assert	inboundPortURI != null && !inboundPortURI.isEmpty() :
				new PreconditionException(
						"inboundPortURI != null && !inboundPortURI.isEmpty()");

		ContentAccessSyncInboundPort p =
						new ContentAccessSyncInboundPort(this.inboundPortURI, c);
		p.publishPort();

		// Postconditions checking
		assert	p != null && p.isPublished() :
				new PostconditionException(
						"return != null && return.isPublished()");
		assert	((AbstractPort)p).getPortURI().equals(inboundPortURI) :
				new PostconditionException(
						"((AbstractPort)return).getPortURI().equals(inboundPortURI)");
		assert	getServerSideInterface().isAssignableFrom(p.getClass()) :
				new PostconditionException(
						"getOfferedComponentInterface()."
						+ "isAssignableFrom(return.getClass())");
		// Invariant checking
		assert	ContentAccessEndPoint.implementationInvariants(this) :
				new ImplementationInvariantException(
						"implementationInvariants(this)");
		assert	ContentAccessEndPoint.invariants(this) :
				new InvariantException("invariants(this)");
		
		return p;
	}
	
	@Override
	protected ContentAccessSyncCI		makeOutboundPort(
		AbstractComponent c,
		String outboundPortURI,
		String inboundPortURI
		) throws Exception
	{
		// Preconditions checking
		assert	c != null : new PreconditionException("c != null");

		ContentAccessSyncOutboundPort p =
				new ContentAccessSyncOutboundPort(outboundPortURI, c);
		p.publishPort();
		c.doPortConnection(
				p.getPortURI(),
				this.inboundPortURI,
				ContentAccessSyncConnector.class.getCanonicalName());

		// Postconditions checking
		assert	p != null && p.isPublished() && p.connected() :
				new PostconditionException(
						"return != null && return.isPublished() && "
						+ "return.connected()");
		assert	((AbstractPort)p).getServerPortURI().equals(getInboundPortURI()) :
				new PostconditionException(
						"((AbstractPort)return).getServerPortURI()."
						+ "equals(getInboundPortURI())");
		assert	getClientSideInterface().isAssignableFrom(p.getClass()) :
				new PostconditionException(
						"getImplementedInterface().isAssignableFrom("
						+ "return.getClass())");
		
		// Invariant checking
		assert	implementationInvariants(this) :
				new ImplementationInvariantException(
						"implementationInvariants(this)");
		assert	invariants(this) : new InvariantException("invariants(this)");
		
		return p;
	}
}
