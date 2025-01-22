package fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend;

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

import java.io.Serializable;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;

/**
 * The interface <code>DHTServicesI</code> defines the methods that the DHT
 * front end must implement.
 *
 * <p><strong>Description</strong></p>
 * 
 * <p>
 * The signatures for {@code get}, {@code put} and {@code remove} correspond
 * to the usual operations on a hash table coarsely conforming to the semantics
 * of the corresponding signatures in the {@code java.util.Map} standard Java
 * interface, while the one for {@code mapReduce} is used to perform map/reduce
 * computations and is coarsely conforming to the semantics of the corresponding
 * signatures in the {@code java.util.stream.Stream} standard Java interface.
 * </p>
 * 
 * <p><strong>Invariants</strong></p>
 * 
 * <pre>
 * invariant	{@code true}	// no invariant
 * </pre>
 * 
 * <p>Created on : 2024-06-27</p>
 * 
 * @author	<a href="mailto:Jacques.Malenfant@lip6.fr">Jacques Malenfant</a>
 */
public interface		DHTServicesI
{
	/**
	 * get the element associated with {@code key} returning {@code null} if it
	 * is absent from the DHT.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code key != null}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param key				the key to the sought value.	
	 * @return					the value associated with {@code key} or {@code null} if the element is absent.
	 * @throws Exception		<i>to do</i>.
	 */
	public ContentDataI	get(ContentKeyI key)
	throws Exception;

	/**
	 * put {@code value} associated with {@code key} in the DHT and return the
	 * previous value associated with {@code key} or {@code null} if it was not
	 * present in the DHT before.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code key != null}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param key				the key to which {@code value} will be associated.
	 * @param value				the value to be added to the DHT.
	 * @return					the previous value associated with {@code key} or {@code null} if the key is absent.
	 * @throws Exception		<i>to do</i>.
	 */
	public ContentDataI	put(ContentKeyI key, ContentDataI value)
	throws Exception;

	/**
	 * remove the value associated with {@code key} from the DHT.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code key != null}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param key				the key to the sought value.
	 * @return					the previous value associated with {@code key} or {@code null} if the key is absent.
	 * @throws Exception		<i>to do</i>.
	 */
	public ContentDataI	remove(ContentKeyI key) throws Exception;

	/**
	 * map the {@code processor} computation over the entries in the DHT that
	 * passes the {@code selector} test and then reduce the results of the map
	 * with {@code reductor} to compute a new accumulator value from a previous
	 * accumulator value and a result from the map, using {@code combinator}
	 * when there is a need to compute a new accumulator value from two
	 * previously computed accumulator values.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code selector != null}
	 * pre	{@code processor != null}
	 * pre	{@code reductor != null}
	 * pre	{@code combinator != null}
	 * pre	{@code initialAcc != null}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param <R>				type of the results of the map computation.
	 * @param selector			a boolean function that selects the entries in the DHT that will be processed by the map.
	 * @param processor			a function implementing the processing to be mapped itself.
	 * @param reductor			function {@code A} x {@code R} -> {@code A} accumulating one map result.
	 * @param combinator		function {@code A} x {@code A} -> {@code A} combining two accumulators.
	 * @param initialAcc		the initial accumulator value.
	 * @return					the final accumulator after all reductions.
	 * @throws Exception		<i>to do</i>.
	 */
	public <R extends Serializable,A extends Serializable> A	mapReduce(
		SelectorI selector,
		ProcessorI<R> processor,
		ReductorI<A,R> reductor,
		CombinatorI<A> combinator,
		A initialAcc
		) throws Exception;
}
