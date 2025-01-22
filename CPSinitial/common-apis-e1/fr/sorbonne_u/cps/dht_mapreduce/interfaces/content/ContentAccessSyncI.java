package fr.sorbonne_u.cps.dht_mapreduce.interfaces.content;

// Copyright Jacques Malenfant, Sorbonne Universite
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

/**
 * The interface <code>ContentAccessSyncI</code> defines the signatures of
 * services used in the DHT nodes to access the content of the DHT stored in
 * the receiving node or one of its follower in the DHT ring.
 *
 * <p><strong>Description</strong></p>
 * 
 * <p>
 * The signatures in this interface coarsely comply with the corresponding ones
 * in the {@code java.util.Map} interface from the Java standard library. The
 * suffix Sync added to the names indicates that their operational semantics
 * is to call the service in a synchronous way, the caller being suspended,
 * waiting for the result, until the called operation returns it. This is the
 * standard call semantics in sequential Java, indeed. HOwever, in subsequent
 * interfaces, asynchronous and parallel semantics will be added. Hence, the
 * signatures provide for an evolution towards parallel execution by having
 * a computation URI parameter identifying the request made to the DHT. The
 * {@code clearComputation} signature intent is to provide for a way to clean
 * up any temporary data that the DHT could have stored during the execution of
 * a request after the request has terminated.
 * </p>
 * 
 * <p><strong>Invariants</strong></p>
 * 
 * <pre>
 * invariant	{@code true}	// no more invariant
 * </pre>
 * 
 * <p>Created on : 2024-06-04</p>
 * 
 * @author	<a href="mailto:Jacques.Malenfant@lip6.fr">Jacques Malenfant</a>
 */
public interface		ContentAccessSyncI
{
	/**
	 * get the element associated with {@code key} returning {@code null} if it
	 * is absent from the DHT.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code computationURI != null && !computationURI.isEmpty()}
	 * pre	{@code key != null}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param computationURI	URI of the computation.
	 * @param key				the key to the sought value.	
	 * @return					the value associated with {@code key} or {@code null} if the element is absent.
	 * @throws Exception		<i>to do</i>.
	 */
	public ContentDataI	getSync(String computationURI, ContentKeyI key)
	throws Exception;

	/**
	 * put {@code value} associated with {@code key} in the DHT and return the
	 * previous value associated with {@code key} or {@code null} if it was not
	 * present in the DHT before.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code computationURI != null && !computationURI.isEmpty()}
	 * pre	{@code key != null}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param computationURI	URI of the computation.
	 * @param key				the key to which {@code value} will be associated.
	 * @param value				the value to be added to the DHT.
	 * @return					the previous value associated with {@code key} or {@code null} if the key is absent.
	 * @throws Exception		<i>to do</i>.
	 */
	public ContentDataI	putSync(
		String computationURI,
		ContentKeyI key,
		ContentDataI value
		) throws Exception;

	/**
	 * remove the value associated with {@code key} from the DHT.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code computationURI != null && !computationURI.isEmpty()}
	 * pre	{@code key != null}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param computationURI	URI of the computation.
	 * @param key				the key to the sought value.
	 * @return					the previous value associated with {@code key} or {@code null} if the key is absent.
	 * @throws Exception		<i>to do</i>.
	 */
	public ContentDataI	removeSync(String computationURI, ContentKeyI key)
	throws Exception;

	/**
	 * clear the computation from the data structures of the DHT after is has
	 * been completed.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code computationURI != null && !computationURI.isEmpty()}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param computationURI	URI of the computation.
	 * @throws Exception		<i>to do</i>.
	 */
	public void			clearComputation(String computationURI)
	throws Exception;
}
