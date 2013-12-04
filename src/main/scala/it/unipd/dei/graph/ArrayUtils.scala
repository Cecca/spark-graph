/*
 * Copyright (C) 2013 Matteo Ceccarello
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU General Public License for more details.
 *
 *     You should have received a copy of the GNU General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package it.unipd.dei.graph

object ArrayUtils {

  /**
   * Returns the difference of two sorted arrays.
   * Performs `a - b`, returning the array with all the elements
   * of `a` that are not contained in `b`.
   */
  def diff(a: Array[NodeId], b: Array[NodeId]): Array[NodeId] = {
    var res: Array[NodeId] = Array(a.length)
    var i = 0
    var aIdx = 0
    var bIdx = 0

    while (aIdx < a.length && bIdx < b.length) {
      res = ensureCapacity(res, i)
      if(a(aIdx) == b(bIdx)) {
        aIdx += 1
        bIdx += 1
      } else if(a(aIdx) < b(bIdx)) {
        res(i) = a(aIdx)
        aIdx += 1
        i += 1
      } else {
        bIdx += 1
      }
    }

    //add all the remaining elements from a, if any
    while (aIdx < a.length) {
      res = ensureCapacity(res, i)
      res(i) = a(aIdx)
      aIdx += 1
      i += 1
    }

    trim(res, i)
  }

  /**
   * Merge two sorted arrays
   */
  def merge(a: Array[NodeId], b: Array[NodeId]): Array[NodeId] = {
    var i = 0
    var aIdx = 0
    var bIdx = 0

    var result: Array[NodeId] = Array.ofDim(a.length+1)

    // merge the first elements
    while(aIdx < a.length && bIdx < b.length) {
      result = ensureCapacity(result, i)
      if (a(aIdx) == b(bIdx)) {
        result(i) = a(aIdx)
        aIdx += 1
        bIdx += 1
      } else if(a(aIdx) < b(bIdx)) {
        result(i) = a(aIdx)
        aIdx += 1
      } else {
        result(i) = b(bIdx)
        bIdx += 1
      }
      i += 1
    }

    // merge the remaining
    while(aIdx < a.length) {
      result = ensureCapacity(result, i)
      result(i) = a(aIdx)
      aIdx += 1
      i += 1
    }
    while(bIdx < b.length) {
      result = ensureCapacity(result, i)
      result(i) = b(bIdx)
      bIdx += 1
      i += 1
    }

    trim(result, i)
  }

  private def ensureCapacity(arr: Array[NodeId], i: Int): Array[NodeId] = {
    if (i < arr.length) {
      arr
    } else {
      val newArr: Array[NodeId] = Array.ofDim(2*arr.length)
      System.arraycopy(arr, 0, newArr, 0, arr.length)
      newArr
    }
  }

  private def trim(arr: Array[NodeId], i: Int): Array[NodeId] = {
    if(i<arr.length){
      val newArr: Array[NodeId] = Array.ofDim(i)
      System.arraycopy(arr, 0, newArr, 0, i)
      newArr
    } else {
      arr
    }
  }

}
