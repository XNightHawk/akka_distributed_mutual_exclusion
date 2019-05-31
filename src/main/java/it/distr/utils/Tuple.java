/**
 *
 *   _|_|_|    _|      _|  _|      _|
 *   _|    _|  _|_|  _|_|    _|  _|
 *   _|    _|  _|  _|  _|      _|
 *   _|    _|  _|      _|    _|  _|
 *   _|_|_|    _|      _|  _|      _|
 *
 *   DMX: A distributed protocol for mutual exclusion
 *
 *   Authors: Willi Menapace      <willi.menapace@studenti.unitn.it>
 *            Daniele Giuliani    <daniele.giuliani@studenti.unitn.it>
 *
 **/

package it.distr.utils;

public class Tuple<X, Y> {
    private final X x;
    private final Y y;

    public Tuple(X x, Y y) {
        this.x = x;
        this.y = y;
    }

    public X first() {
        return this.x;
    }

    public Y last() {
        return this.y;
    }
}