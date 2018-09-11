package com.github.animeshtrivedi.anoc;

/**
 * Created by atr on 15.08.18.
 */
public class JavaMain {
    public static void main(String[] args) {
        ScalaMain sm = new ScalaMain();
        System.out.println("Hello World! " + ScalaMain.name() + sm.getClass().getCanonicalName()); // Display the string.
    }
}
