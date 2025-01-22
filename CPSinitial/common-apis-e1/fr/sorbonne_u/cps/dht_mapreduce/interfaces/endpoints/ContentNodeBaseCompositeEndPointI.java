package fr.sorbonne_u.cps.dht_mapreduce.interfaces.endpoints;

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

import fr.sorbonne_u.components.endpoints.CompositeEndPointI;
import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncI;

/**
 * The interface <code>ContentNodeBaseCompositeEndPointI</code> defines the
 * signatures of methods that a composite of several end points connecting a
 * client to a server implementation of a content node in the distributed hash
 * table must provide.
 *
 * <p><strong>Description</strong></p>
 * 
 * <p>
 * A content node has two connection points, or end points, corresponding
 * to two interfaces that a server provides through the composite: a content
 * access interface for adding, getting and removing data from the DHT and
 * map/reduce interface for performing data-wise computations. The composite
 * end point, used to facilitate the connections among clients of these end
 * points and nodes providing them in the DHT, allows to retrieve each simple
 * end points (of type {@code EndPointI}) for each of the interfaces provided
 * through these end points in the composite.
 * </p>
 * <p>
 * The signatures in this interface also specialise signatures declared in
 * {@code CompositeEndPointI} to the types used in DHT nodes. The signature
 * {@code copyWithSharable} is added merely to specialise the return type
 * of the overwritten signature from {@code CompositeEndPointI} with the
 * hereby defined interface. Doing this avoids class casts in the code using
 * this signature.
 * </p>
 * 
 * <p><strong>Invariants</strong></p>
 * 
 * <pre>
 * invariant	{@code true}	// no more invariant
 * </pre>
 * 
 * <p>Created on : 2024-06-24</p>
 * 
 * @author	<a href="mailto:Jacques.Malenfant@lip6.fr">Jacques Malenfant</a>
 */
public interface		ContentNodeBaseCompositeEndPointI<
											CAI  extends ContentAccessSyncI,
											MRI extends MapReduceSyncI>
extends		CompositeEndPointI
{
	/**
	 * return the end point corresponding to a {@code ContentAccessCI}
	 * connection.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code clientSideInitialised()}
	 * post	{@code return != null}
	 * </pre>
	 *
	 * @return	the end point corresponding to a {@code ContentAccessCI} connection.
	 */
	public EndPointI<CAI>	getContentAccessEndpoint();

	/**
	 * return the end point corresponding to a {@code MapReduceCI}
	 * connection.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code clientSideInitialised()}
	 * post	{@code return != null}
	 * </pre>
	 *
	 * @return	the end point corresponding to a {@code MapReduceCI} connection.
	 */
	public EndPointI<MRI>	getMapReduceEndpoint();

	/**
	 * duplicate this content node multiple end points except its transient
	 * information <i>i.e.</i>, keeping only the information that is shared
	 * among copies of the multiple end point.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code true}	// no more preconditions.
	 * post	{@code return != null}
	 * </pre>
	 * 
	 * @see fr.sorbonne_u.components.endpoints.MultiEndPointsI#copyWithSharable()
	 */
	@Override
	public ContentNodeBaseCompositeEndPointI<CAI,MRI>	copyWithSharable();
}
