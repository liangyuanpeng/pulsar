package org.apache.pulsar.functions.worker;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;

public class LoadClassDemo {
    public static void main(String[] args) throws MalformedURLException, ClassNotFoundException {
        File jar = new File("/disk/server/apache-pulsar-2.5.0/examples/api-examples.jar");
        java.net.URL url = jar.toURI().toURL();
        ClassLoader clsLoader = new URLClassLoader(new URL[]{url});
        Class functionClass = clsLoader.loadClass("org.apache.pulsar.functions.api.examples.ExclamationFunction");
        System.out.println(functionClass.getCanonicalName());

    }
}
