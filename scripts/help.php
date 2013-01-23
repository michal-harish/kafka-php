<?php

/**
 * Help
 *
 * Simple method that will display the script's help message, in case
 * some of the required parameters are missing or non are given.
 *
 * It fetches the information from the script that is being called, so
 * we can re-use the same method for all the scripts.
 */
function help()
{
    global $argv;

    // get the script path from the global arguments vector
    $script = $argv[0];
    $scriptName = end(explode("/", $script));

    // get the help from the class description
    exec("grep -P '\*\.' < $script | cut -c5-", $output);
    $help = implode ("\n", $output);

    // output the help
    echo "\n\033[1;32m$scriptName\033[0m\n";
    echo "\n$help\n";
    echo "\nUsage: $script -c {connector} -t {topic}\n\n";
    exit(1);
}
