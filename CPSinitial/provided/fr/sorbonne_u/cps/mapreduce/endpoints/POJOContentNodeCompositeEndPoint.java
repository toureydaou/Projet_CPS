package fr.sorbonne_u.cps.mapreduce.endpoints;

// Copyright Jacques Malenfant, Sorbonne Universite.
// Jacques.Malenfant@lip6.fr
//
// This software is a computer program whose purpose is to provide an example
// of a component-based distributed application, namely a Distributed Hash Table
// over which a Map/Reduce processing capability is added.
//
// This software is governed by the CeCILL-C license under French law and
// abiding by the rules of distribution of free software.  You can use,
// modify and/ or redistribute the software under the terms of the
// CeCILL-C license as circulated by CEA, CNRS and INRIA at the following
// URL "http://www.cecill.info".
//
// As a counterpart to the access to the source code and  rights to copy,
// modify and redistribute granted by the license, users are provided only
// with a limited warranty  and the software's author,  the holder of the
// economic rights,  and the successive licensors  have only  limited
// liability. 
//
// In this respect, the user's attention is drawn to the risks associated
// with loading,  using,  modifying and/or developing or reproducing the
// software by the user in light of its specific status of free software,
// that may mean  that it is complicated to manipulate,  and  that  also
// therefore means  that it is reserved for developers  and  experienced
// professionals having in-depth computer knowledge. Users are therefore
// encouraged to load and test the software's suitability as regards their
// requirements in conditions enabling the security of their systems and/or 
// data to be ensured and,  more generally, to use and operate it in the 
// same conditions as regards security. 
//
// The fact that you are presently reading this means that you have had
// knowledge of the CeCILL-C license and that you accept its terms.

import fr.sorbonne_u.exceptions.PostconditionException;
import fr.sorbonne_u.exceptions.PreconditionException;
import fr.sorbonne_u.components.endpoints.CompositeEndPoint;
import fr.sorbonne_u.components.endpoints.POJOEndPoint;
import fr.sorbonne_u.components.exceptions.BCMException;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.endpoints.ContentNodeBaseCompositeEndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncI;

/**
 * The class <code>POJOContentNodeDescriptor</code> implements a composite
 * content node end point for an implementation of the DHT with plain old
 * Java objects (POJO).
 *
 * <p><strong>Description</strong></p>
 * 
 * <p><strong>Implementation Invariants</strong></p>
 * 
 * <pre>
 * invariant	{@code true}	// no more invariant
 * </pre>
 * 
 * <p><strong>Invariants</strong></p>
 * 
 * <pre>
 * invariant	{@code true}	// no more invariant
 * </pre>
 * 
 * <p>Created on : 2024-06-25</p>
 * 
 * @author	<a href="mailto:Jacques.Malenfant@lip6.fr">Jacques Malenfant</a>
 */
public class			POJOContentNodeCompositeEndPoint
extends		CompositeEndPoint
implements	ContentNodeBaseCompositeEndPointI<ContentAccessSyncI,MapReduceSyncI>
{
	// -------------------------------------------------------------------------
	// Constants and variables
	// -------------------------------------------------------------------------

	/** number of end points in this multi end points.						*/
	protected static final int			NUMBER_OF_ENDPOINTS = 2;

	// -------------------------------------------------------------------------
	// Constructors
	// -------------------------------------------------------------------------

	/**
	 * create a new node descriptor.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code complete()}
	 * post	{@code !serverSideInitialised()}
	 * post	{@code !clientSideInitialised()}
	 * </pre>
	 *
	 */
	public				POJOContentNodeCompositeEndPoint()
	{
		super(NUMBER_OF_ENDPOINTS);

		POJOEndPoint<ContentAccessSyncI> contentAccessEndpoint =
				new POJOEndPoint<ContentAccessSyncI>(ContentAccessSyncI.class);
		this.addEndPoint(contentAccessEndpoint);
		POJOEndPoint<MapReduceSyncI> mapReduceSyncEndPoint =
				new POJOEndPoint<MapReduceSyncI>(MapReduceSyncI.class);
		this.addEndPoint(mapReduceSyncEndPoint);

		assert	complete() :
				new PostconditionException("complete()");
		assert	!serverSideInitialised() :
				new PostconditionException("!serverSideInitialised()");
		assert	!clientSideInitialised() :
				new PostconditionException("!clientSideInitialised()");
	}

	// -------------------------------------------------------------------------
	// Methods
	// -------------------------------------------------------------------------

	/**
	 * @see fr.sorbonne_u.components.endpoints.MultiEndPoints#complete()
	 */
	@Override
	public boolean		complete()
	{
		return	super.complete() &&
				this.hasImplementedInterface(ContentAccessSyncI.class) &&
				this.hasImplementedInterface(MapReduceSyncI.class);
	}

	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.endpoints.ContentNodeBaseCompositeEndPointI#initialiseServerSide(java.lang.Object)
	 */
	@Override
	public void			initialiseServerSide(Object serverSideEndPointOwner)
	{
		assert	!serverSideInitialised() :
				new PreconditionException("!serverSideInitialised()");
		assert	serverSideEndPointOwner != null :
				new PreconditionException("serverSideEndPointOwner != null");

		assert	serverSideEndPointOwner instanceof ContentAccessSyncI :
				new BCMException(
						"serverSideEndPointOwner instanceof ContentAccessSyncI");
		assert	serverSideEndPointOwner instanceof MapReduceSyncI :
				new BCMException(
						"serverSideEndPointOwner instanceof MapReduceSyncI");

		super.initialiseServerSide(serverSideEndPointOwner);

		assert	serverSideInitialised() :
				new PostconditionException("serverSideInitialised()");
	}

	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.endpoints.ContentNodeBaseCompositeEndPointI#getContentAccessEndpoint()
	 */
	@Override
	public POJOEndPoint<ContentAccessSyncI>	getContentAccessEndpoint()
	{
		assert	clientSideInitialised() :
				new PreconditionException("clientSideInitialised()");

		return (POJOEndPoint<ContentAccessSyncI>)
								this.getEndPoint(ContentAccessSyncI.class);
	}

	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.endpoints.ContentNodeBaseCompositeEndPointI#getMapReduceEndpoint()
	 */
	@Override
	public POJOEndPoint<MapReduceSyncI>	getMapReduceEndpoint()
	{
		assert	clientSideInitialised() :
		new PreconditionException("clientSideInitialised()");

		return (POJOEndPoint<MapReduceSyncI>)
								this.getEndPoint(MapReduceSyncI.class);
	}

	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.endpoints.ContentNodeBaseCompositeEndPointI#copyWithSharable()
	 */
	@SuppressWarnings("unchecked")
	@Override
	public ContentNodeBaseCompositeEndPointI<ContentAccessSyncI, MapReduceSyncI>
										copyWithSharable()
	{
		return (ContentNodeBaseCompositeEndPointI<ContentAccessSyncI,
												  MapReduceSyncI>)
							super.copyWithSharable();
	}
}
