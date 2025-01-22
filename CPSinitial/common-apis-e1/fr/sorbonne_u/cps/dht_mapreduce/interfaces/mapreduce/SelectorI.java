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

import java.util.function.Predicate;

import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;

import java.io.Serializable;

/**
 * The functional interface <code>SelectorI</code> allows to define functions
 * in the map part of the map/reduce protocol that select individual data of
 * type {@code ContentDataI} that will be processed.
 *
 * <p><strong>Description</strong></p>
 * 
 * <p>
 * In the map part of the map/reduce, each data present in the DHT is reviewed
 * for potential processing by a selection function. The selector functions in
 * this DHT/MapReduce project must implement {@code SelectorI}, which is a
 * specialisation of the Java functional interface
 * {@code Predicate<ContentDataI>} that takes a parameter that is a piece of
 * data contained in the DHT and returns true if this piece of data must be
 * processed in the current map/reduce computation and false otherwise.
 * </p>
 * <p>
 * The {@code SelectorI} functional interface is itself serialisable as in a
 * DHT, they have to be passed as parameters over the network.
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
@FunctionalInterface
public interface  		SelectorI
extends		Predicate<ContentDataI>,
			Serializable
{

}
