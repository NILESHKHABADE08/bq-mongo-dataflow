import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from pymongo import MongoClient
import argparse

class WriteToMongoDB(beam.DoFn):

    def __init__(self, mongo_uri, database, collection):
        self.mongo_uri = mongo_uri
        self.database = database
        self.collection_name = collection

    def setup(self):
        self.client = MongoClient(self.mongo_uri)
        self.db = self.client[self.database]
        self.collection = self.db[self.collection_name]

    def process(self, element):
        doc = dict(element)

        if "_id" in doc:
            doc["_id"] = str(doc["_id"])

        self.collection.insert_one(doc)

        yield doc


def run():

    parser = argparse.ArgumentParser()

    parser.add_argument('--project')
    parser.add_argument('--region')
    parser.add_argument('--input_table')
    parser.add_argument('--mongo_uri')
    parser.add_argument('--mongo_db')
    parser.add_argument('--mongo_collection')
    parser.add_argument('--temp_location')
    parser.add_argument('--staging_location')

    args, beam_args = parser.parse_known_args()

    pipeline_options = PipelineOptions(
        beam_args,
        project=args.project,
        region=args.region,
        temp_location=args.temp_location,
        staging_location=args.staging_location,
        runner="DataflowRunner",
        save_main_session=True
    )

    with beam.Pipeline(options=pipeline_options) as p:

        (
            p
            | "Read from BigQuery" >> beam.io.ReadFromBigQuery(
                table=args.input_table
            )
            | "Write to MongoDB" >> beam.ParDo(
                WriteToMongoDB(
                    args.mongo_uri,
                    args.mongo_db,
                    args.mongo_collection
                )
            )
        )


if __name__ == "__main__":
    run()
