package com.github.pramod.kafka.twitterproducer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;


/**
 * @author Crunchify dot com ---- the code is from this site originally and was modified
 *
 * To do: Change to singleton
 */

public class GetPropertyValues {

        String result = "";
        InputStream inputStream;

        private Logger logger = LoggerFactory.getLogger(GetPropertyValues.class.getName());

        public String getPropValues(String key) throws IOException {

            try {
                Properties prop = new Properties();
                String propFileName = "config.properties";

                inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);

                if (inputStream != null) {
                    prop.load(inputStream);
                } else {
                    throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
                }

                // get the property value and print it out
                result = prop.getProperty(key);


            } catch (Exception e) {
                logger.error("Exception: " + e);
            } finally {
                inputStream.close();
            }
            return result;
        }

}
