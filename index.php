<?php

function parsePostRequest()
{
    // Get the raw POST data
    $post_data = file_get_contents('php://input');

    // Parse JSON data
    $parsed_data = json_decode($post_data, true);

    // Check if data is not empty and contains required fields
    if ($parsed_data && isset($parsed_data['table'], $parsed_data['column'], $parsed_data['query'])) {
        // Extract the required fields
        $table = $parsed_data['table'];
        $column = $parsed_data['column'];
        $query = $parsed_data['query'];

        // Extract optional fields if they exist, otherwise set default values
        $limit = isset($parsed_data['limit']) ? $parsed_data['limit'] : null;
        $fuzzy = isset($parsed_data['fuzzy']) ? $parsed_data['fuzzy'] : null;
        $andOp = isset($parsed_data['andOp']) ? $parsed_data['andOp'] : null;
        $offset = isset($parsed_data['offset']) ? $parsed_data['offset'] : null;

        // Return the extracted data
        return [
            'table' => $table,
            'column' => $column,
            'query' => $query,
            'limit' => $limit,
            'offset' => $offset,
            'fuzzy' => $fuzzy,
            'andOp' => $andOp
        ];
    } else {
        // If required fields are missing, return an error or handle it according to your needs
        return null;
    }
}

$data = parsePostRequest();

// Build the base command
$command = "ffsearch.exe search -t \"{$data['table']}\" -c \"{$data['column']}\" -s \"{$data['query']}\"";

// Add optional parameters if provided
if ($data['limit'] !== null) {
    $command .= " -l \"{$data['limit']}\"";
}

if ($data['offset'] !== null) {
    $command .= " -o \"{$data['offset']}\"";
}

if ($data['fuzzy'] !== null) {
    $command .= " -f \"{$data['fuzzy']}\"";
}

if ($data['andOp'] !== null) {
    $command .= " -a \"{$data['andOp']}\"";
}

header('Content-Type: application/json');
echo shell_exec($command);
