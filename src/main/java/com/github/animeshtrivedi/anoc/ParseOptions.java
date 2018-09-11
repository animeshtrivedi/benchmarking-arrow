package com.github.animeshtrivedi.anoc;

import org.apache.commons.cli.*;

/**
 * Created by atr on 9/30/16.
 */
public class ParseOptions {
    private Options options;

    public ParseOptions(){
        options = new Options();
        options.addOption("h", "help", false, "show help.");
    }

    public void show_help() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("Main", options);
    }
    public void parse(String[] args) {
        CommandLineParser parser = new GnuParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);

            if (cmd.hasOption("h")) {
                show_help();
                System.exit(0);
            }
        } catch (ParseException e) {
            System.err.println("Failed to parse command line properties" + e);
            show_help();
            System.exit(-1);
        }
    }
}
