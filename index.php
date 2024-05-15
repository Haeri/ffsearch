<?php

header('Content-Type: application/json');

// Get the raw POST data
$post_data = file_get_contents('php://input');

// Parse JSON data
$parsed_data = json_decode($post_data, true);


// Build the base command
$command = "./ffsearch search";

// Add optional parameters if provided
if (!empty($parsed_data['table'])) {
    $command .= " -t \"{$parsed_data['table']}\"";
}
if (!empty($parsed_data['column'])) {
    $command .= " -c \"{$parsed_data['column']}\"";
}
if (!empty($parsed_data['query'])) {
    $command .= " -s \"{$parsed_data['query']}\"";
}
if (!empty($parsed_data['limit'])) {
    $command .= " -l \"{$parsed_data['limit']}\"";
}
if (!empty($parsed_data['offset'])) {
    $command .= " -o \"{$parsed_data['offset']}\"";
}
if (!empty($parsed_data['fuzzy'])) {
    $command .= " -f \"{$parsed_data['fuzzy']}\"";
}
if (!empty($parsed_data['andOp'])) {
    $command .= " -a \"{$parsed_data['andOp']}\"";
}

echo shell_exec($command);
