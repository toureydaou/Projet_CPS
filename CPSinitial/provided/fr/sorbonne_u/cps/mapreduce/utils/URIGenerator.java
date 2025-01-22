package fr.sorbonne_u.cps.mapreduce.utils;

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

/**
 * The abstract class <code>URIGenerator</code> provides an implementation of
 * an URI generator method {@code generateURI}.
 *
 * <p><strong>Description</strong></p>
 * 
 * <p>
 * The method {@code generateURI} uses a standard URI generation provided by
 * Java @see http://www.asciiarmor.com/post/33736615/java-util-uuid-mini-faq.
 * Two second method allow to add a prefix to the URI, usually to have a more
 * readable URI for debugging purposes.
 * </p>
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
 * <p>Created on : 2024-12-09</p>
 * 
 * @author	<a href="mailto:Jacques.Malenfant@lip6.fr">Jacques Malenfant</a>
 */
public abstract class	URIGenerator
{
	// -------------------------------------------------------------------------
	// Methods
	// -------------------------------------------------------------------------

	/**
	 * generate an URI, used to identify computations executed on the DHT ring.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code return != null && !return.isEmpty()}
	 * </pre>
	 *
	 * @return	a new URI.
	 */
	public static String	generateURI()
	{
		// see http://www.asciiarmor.com/post/33736615/java-util-uuid-mini-faq
		return java.util.UUID.randomUUID().toString();
	}

	/**
	 * generate an URI, used to identify computations executed on the DHT ring.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code prefix != null}
	 * post	{@code return != null && !return.isEmpty()}
	 * </pre>
	 *
	 * @param prefix		prefix to be added to the returned URI.
	 * @return				a new URI beginning with the content of {@code prefix}.
	 */
	public static String	generateURI(String prefix)
	{
		return prefix + "-" + generateURI();
	}
}
