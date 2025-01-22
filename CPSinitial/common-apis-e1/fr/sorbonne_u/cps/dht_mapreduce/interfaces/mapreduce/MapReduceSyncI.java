package fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce;

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

/**
 * The interface <code>MapReduceSyncI</code> defines the methods implementing
 * the map and the reduce computations over the DHT.
 *
 * <p><strong>Description</strong></p>
 * 
 * <p>
 * This interface is used as an implementation interface for nodes in the DHT
 * providing a map/reduce facility over the entries of the DHT. The computation
 * is done in two phases: first the map traverses the DHT nodes and then the
 * reduce. To enable a parallelism between the map/reduce computations, an URI
 * is attributed to each request so that the implementation  can use it to
 * perform the DHT-wise operation and store intermediate results. The signature
 * clearMapReduceComputation provides for a way to clean up any remaining
 * intermediate data used to compute a request after it has terminated.
 * </p>
 * <p>
 * As indicated by the suffix Sync on the signature names, this map/reduce
 * interface imposes that the computations for the map and the reduce will be
 * executed through synchronous calls <i>i.e.</i>, callers are suspended until
 * the results come back from the callee, like in standard sequential method
 * calls.
 * </p>
 * 
 * <p><strong>White-box Invariant</strong></p>
 * 
 * <pre>
 * invariant	{@code true}	// no more invariant
 * </pre>
 * 
 * <p>Created on : 2024-06-04</p>
 * 
 * @author	<a href="mailto:Jacques.Malenfant@lip6.fr">Jacques Malenfant</a>
 */
public interface		MapReduceSyncI
{
	/**
	 * map the {@code processor} computation over the entries in the DHT that
	 * passes the {@code selector} test.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code computationURI != null && !computationURI.isEmpty()}
	 * pre	{@code selector != null}
	 * pre	{@code processor != null}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param <R>				type of the results of the map computation.
	 * @param computationURI	URI of the computation, used to distinguish parallel maps over the same DHT.
	 * @param selector			a boolean function that selects the entries in the DHT that will be processed by the map.
	 * @param processor			a function implementing the processing to be mapped itself.
	 * @throws Exception		<i>to do</i>.
	 */
	public <R extends Serializable> void	mapSync(
		String computationURI,
		SelectorI selector,
		ProcessorI<R> processor
		) throws Exception;

	/**
	 * synchronously reduce the results of type {@code R} from a previously
	 * executed map with URI {@code computationURI} using an accumulator of type
	 * {@code A}; {@code reductor} is the function that computes a new
	 * accumulator value from the previous one and a map result while
	 * {@code combinator} is the function that computes a new accumulator value
	 * from two previous accumulator values.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code computationURI != null && !computationURI.isEmpty()}
	 * pre	{@code reductor != null}
	 * pre	{@code combinator != null}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param <A>				type of the accumulator.
	 * @param <R>				type of the map results.
	 * @param <I>				type of the result reception interface.
	 * @param computationURI	URI of the computation, used to distinguish parallel maps over the same DHT.
	 * @param reductor			function {@code A} x {@code R} -> {@code A} accumulating one map result.
	 * @param combinator		function {@code A} x {@code A} -> {@code A} combining two accumulators.
	 * @param currentAcc		the initial accumulator value.
	 * @return					the final accumulator after all reductions.
	 * @throws Exception		<i>to do</i>.
	 */
	public <A extends Serializable,R> A		reduceSync(
		String computationURI,
		ReductorI<A,R> reductor,
		CombinatorI<A> combinator,
		A currentAcc
		) throws Exception;

	/**
	 * clear the map/reduce computation from the data structures of the DHT
	 * after is has been completed.
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
	public void			clearMapReduceComputation(String computationURI)
	throws Exception;
}
