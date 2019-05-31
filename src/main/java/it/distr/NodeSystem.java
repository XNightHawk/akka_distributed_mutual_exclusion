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

package it.distr;

import akka.actor.ActorSystem;
import it.distr.utils.CommandParser;


public class NodeSystem {

  public static void main(String[] args) {

    // Create the actor system

    final ActorSystem system = ActorSystem.create("network");

    CommandParser cp = new CommandParser(system);

    boolean over = false;

    while(!over) {
      over = cp.parse();
    }

    system.terminate();
  }
}
