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

import java.io.Serializable;
import fr.sorbonne_u.exceptions.PreconditionException;

/**
 * The class <code>LongInterval</code> represent a closed interval of long
 * values.
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
 * invariant	{@code last() > first()}
 * </pre>
 * 
 * <p>Created on : 2024-06-24</p>
 * 
 * @author	<a href="mailto:Jacques.Malenfant@lip6.fr">Jacques Malenfant</a>
 */
public class			IntInterval
implements	Serializable,
			Cloneable
{
	private static final long serialVersionUID = 1L;
	/** first value in the interval.										*/
	protected int		first;
	/** last value in the interval.											*/
	protected int		last;

	/**
	 * create a new interval.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code last > first}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param first	first value in the interval.
	 * @param last	last value in the interval.
	 */
	public IntInterval(int first, int last) {
		super();

		assert	last > first : new PreconditionException("last > first");

		this.first = first;
		this.last = last;
	}

	/**
	 * return the first value in the interval.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @return	the first value in the interval.
	 */
	public int			first()
	{
		return this.first;
	}

	/**
	 * return the last value in the interval.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @return	the last value in the interval.
	 */
	public int			last()
	{
		return this.last;
	}

	/**
	 * return true if {@code value} is in the (closed) interval and false
	 * otherwise.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param value	value to be tested.
	 * @return		true if {@code value} is in the (closed) interval and false otherwise.
	 */
	public boolean		in(int value)
	{
		return value >= first && value <= last;
	}

	/**
	 * return true if the interval contains enough values to be split in two
	 * sub-intervals.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @return	true if the interval contains enough values to be split in two sub-intervals.
	 */
	public boolean		splitable()
	{
		return this.last - 2 > this.first;
	}

	/**
	 * split this interval in two equal parts, updating this interval to be
	 * the first and returning the following intervaL (the second part); this
	 * interval must contain at least four values otherwise the result will be
	 * null as the split cannot occur.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @return	the new interval representing the second part of this interval before the split.
	 */
	public IntInterval	split()
	{
		if (this.last - 2 > this.first) {
			// compute in double to avoid overflow
			double f = this.first;
			double l = this.last;
			int mid = (int) ((f + l)/2.0);
			IntInterval res = new IntInterval(mid+1, this.last);
			this.last = mid;
			return res;
		} else {
			return null;
		}
	}

	/**
	 * merge two contiguous intervals into one, represented by this interval.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code last() + 1 == following.first()}
	 * post	{@code first() == first_pre && last() == following_last_pre}
	 * </pre>
	 *
	 * @param following	interval that follows immediately this interval.
	 */
	public void			merge(IntInterval following)
	{
		assert	this.last() + 1 == following.first();
		int first_pre = this.first();
		int following_last_pre = following.last();

		this.last = following.last();

		assert	this.first() == first_pre && this.last() == following_last_pre;
	}

	/**
	 * clone this integer interval object.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @return	a clone of this integer interval object.
	 */
	public IntInterval	clone()
	{
		return new IntInterval(this.first, this.last);
	}

	/**
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String		toString()
	{
		StringBuffer sb = new StringBuffer(this.getClass().getSimpleName());
		sb.append('[');
		sb.append(this.first);
		sb.append(", ");
		sb.append(this.last);
		sb.append(']');
		return sb.toString();
	}
}
