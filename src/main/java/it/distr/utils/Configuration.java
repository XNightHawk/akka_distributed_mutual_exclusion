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

public class Configuration {
    //Enables message delaying and tracing
    public static final boolean DEBUG = true;
    //Maximum time in ms that a message can be delayed before transmission
    public static final int MAX_WAIT = 250;
    //ANSI CODES FOR COLORED OUTPUT
    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_BLACK = "\u001B[30m";
    public static final String ANSI_RED = "\u001B[31m";
    public static final String ANSI_GREEN = "\u001B[32m";
    public static final String ANSI_YELLOW = "\u001B[33m";
    public static final String ANSI_BLUE = "\u001B[34m";
    public static final String ANSI_PURPLE = "\u001B[35m";
    public static final String ANSI_CYAN = "\u001B[36m";
    public static final String ANSI_WHITE = "\u001B[37m";
}
