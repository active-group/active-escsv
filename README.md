# active-escsv

Export from Elasticsearch to CSV files and import CSV files to Elasticsearch.

## Usage

Export from Elasticsearch to CSV file

    lein run -m active-escsv.export

Import a CSV file to Elasticsearch

    lein run -m active-escsv.import

Both commands accept a `--help` command line argument that prints detailed usage
instructions.

## License

Copyright © 2019 Active Group GmbH

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
