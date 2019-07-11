package com;

import org.apache.log4j.PropertyConfigurator;
import org.junit.Test;

import java.nio.file.Paths;

import static org.junit.Assert.*;

public class JPServerTest {

    @Test
    public void main() throws InterruptedException {
        System.setProperty("JPPath", System.getProperty("user.dir"));
        PropertyConfigurator.configure(Paths.get(System.getProperty("user.dir"),
                "conf", "log4j.properties").toString());
        JPServer.main(null);
    }
}