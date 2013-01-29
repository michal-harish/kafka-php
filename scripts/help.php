<?php

/**
 * Help
 *
 * Simple method that will display the script's help message, in case
 * some of the required parameters are missing or non are given.
 *
 * It fetches the information from the script that is being called, so
 * we can re-use the same method for all the scripts.
 *
 * @author Pau Gay <pau.gay@visualdna.com>
 * @date   2013-01-23
 */

function help($showWrongParameters = true)
{
    global $argv;

    // get the script path from the global arguments vector
    $script = $argv[0];

    // get the help from the class description
    exec("grep -P '\ \*\.' < $script | cut -c5-", $output);
    $help = implode ("\n",
        array_merge(
            array("\033[1;32m" . array_shift($output) . "\033[0m"),
            $output
        )
    );

    // echo the cruft
    if ($showWrongParameters) {
        echo "\n\033[1;31mError:\033[0m Wrong parameter usage.\n";
    }
    echo "\n$help\n\n";
    exit(1);
}

// check if we want to display the help
if (isset($options["h"]) && $options["h"] === false) help(false);
