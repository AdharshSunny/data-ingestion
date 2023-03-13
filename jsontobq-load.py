import apache_beam as beam
import logging
import argparse
import re
import json

REVIEW_TABLE = 'adharsh-sunny-pipelines.coding.REVIEW'

BUSINESS_SCHEMA = {
    'fields': [{
        'name': 'business_id', 'type': 'STRING', 'mode': 'REQUIRED'
    }, {
        'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'
    },
    {
        'name': 'address', 'type': 'STRING', 'mode': 'NULLABLE'
    }, {
        'name': 'city', 'type': 'STRING', 'mode': 'NULLABLE'
    },
     {
        'name': 'state', 'type': 'STRING', 'mode': 'NULLABLE'
    }, {
        'name': 'postal_code', 'type': 'STRING', 'mode': 'NULLABLE'
    },
    {
        'name': 'latitude', 'type': 'FLOAT64', 'mode': 'NULLABLE'
    }, {
        'name': 'longitude', 'type': 'FLOAT64', 'mode': 'NULLABLE'
    },
     {
        'name': 'stars', 'type': 'FLOAT64', 'mode': 'NULLABLE'
    },
    {
        'name': 'review_count', 'type': 'INT64', 'mode': 'NULLABLE'
    },
     {
        'name': 'is_open', 'type': 'INT64', 'mode': 'NULLABLE'
    },
    {
        'name': 'categories', 'type': 'STRING', 'mode': 'NULLABLE'
    },
	{
        'name': 'hours', 'type': 'RECORD', 'mode': 'NULLABLE',"fields": [
            {
                "name": "Monday",
                "type": "STRING"
            },
            {
                "name": "Tuesday",
                "type": "STRING"
            },
            {
                "name": "Friday",
                "type": "STRING"
           },
            {
                "name": "Wednesday",
                "type": "STRING"
            },
            {
               "name": "Thursday",
                "type": "STRING"
           },
           {
               "name": "Sunday",
                "type": "STRING"
            },
            {
                "name": "Saturday",
                "type": "STRING"
            }
        ]
    }
     
    ]
}

class ParseMessage(beam.DoFn):

	def process(self, element):
		line = json.loads(element)
		#print(line)
		business = {
			'business_id' : line['business_id'],
			'name' : line['name'] ,
			'address' : line['address'] ,
			'city' : line['city'] ,
			'state' : line['state'] ,
			'postal_code' : line['postal_code'] ,
			'latitude' : line['latitude'],
			'longitude' : line['longitude'],
			'stars' : line['stars'],
			'review_count' : line['review_count'],
			'is_open' : line['is_open'],
			'categories' : line['categories'],
			'hours': line['hours']
		
		}
		yield business

def run(argv=None):
    parser= argparse.ArgumentParser()
    parser.add_argument('--input',
                        dest='input',
                        required=True,
                        help='Input file to Process')

    known_args,pipeline_args = parser.parse_known_args(argv)

    with beam.Pipeline(argv=pipeline_args) as p:
        lines =(
            p
            | "ReadCSVFile" >> beam.io.ReadFromText(known_args.input)
			| "Parse JSON messages" >> beam.ParDo(ParseMessage())
			| "write_to_BQ" >> beam.io.WriteToBigQuery(
				table=REVIEW_TABLE,
				schema = BUSINESS_SCHEMA,
				create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
				write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
				)
            )

      
if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
 