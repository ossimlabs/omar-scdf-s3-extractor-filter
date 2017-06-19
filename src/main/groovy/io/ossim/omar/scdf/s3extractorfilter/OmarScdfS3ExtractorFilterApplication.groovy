package io.ossim.omar.scdf.s3extractorfilter

import groovy.json.JsonException
import groovy.util.logging.Slf4j
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.cloud.stream.annotation.EnableBinding
import org.springframework.cloud.stream.annotation.StreamListener
import org.springframework.cloud.stream.messaging.Processor
import org.springframework.messaging.Message
import org.springframework.messaging.handler.annotation.SendTo
import groovy.json.JsonSlurper
import groovy.json.JsonBuilder

/**
 * Created by slallier on 6/19/2017
 */
@SpringBootApplication
@EnableBinding(Processor.class)
@Slf4j
class OmarScdfS3ExtractorFilterApplication
{
	/**
	 * The main entry point of the SCDF S3 Extractor Filter application.
	 * @param args
	 */
	static final void main(String[] args)
	{
	       SpringApplication.run OmarScdfS3ExtractorFilterApplication, args
	}

	/**
	 * Receives a message from a SCDF Extractor when an image is extracted.
     * Sends a message with the path to the directory containing the image.
	 *
	 * @param message The message from the extractor
	 * @return a JSON message of directoryPath for the directory
	 */
	@StreamListener(Processor.INPUT)
	@SendTo(Processor.OUTPUT)
	final String filter(final Message<?> message)
	{
        log.debug("Message received: ${message}")

        try
        {
            final def parsedJson = new JsonSlurper().parseText(message.payload)
            final String filename = parsedJson.filename
			final String directoryPath = filename[0..filename.lastIndexOf('/') - 1]

            final JsonBuilder directoryJson = new JsonBuilder()
            directoryJson(
                directoryPath: directoryPath
            )

            log.debug("Parsed directory path:\n" + directoryJson.toString())
    		return directoryJson.toString()
        }
        catch (JsonException jsonEx)
        {
            log.warn("Message received is not in proper JSON format, skipping\n   Message body: ${message}")
    		return null
        }
	}
}
