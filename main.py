import argparse
import logging
import re
import uuid
import math

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


class GenerateWordRecords(beam.DoFn):

    N_REPEATS = 100_000  # Factor to artificially increase the number of elements

    def process(self, element):
        for word in re.findall(r"[\w\']+", element, re.UNICODE):
            for _ in range(self.N_REPEATS):
                yield word, {
                    "word": word,
                    "uuid": str(uuid.uuid4()),
                    **{
                        # NOTE: Meaningless fields to simulate a bigger payload
                        "field_" + str(i): str(uuid.uuid4())
                        for i in range(60)
                    },
                }


def str_entropy(string: str) -> float:
    """Some calculation to simulate some workload."""
    prob = [float(string.count(c)) / len(string) for c in dict.fromkeys(list(string))]
    entropy = -sum([p * math.log(p) / math.log(2.0) for p in prob])
    return entropy


def ord_sum(string: str) -> int:
    """Some more calculation to simulate some workload."""
    return sum(ord(char) for char in string)


class PropagateEntropyToGroup(beam.DoFn):
    """Meaningless function to simulate some workload."""

    def process(self, element):
        word, records = element
        avg_entropy = sum(str_entropy(record["uuid"]) for record in records) / len(
            records
        )
        for record in records:
            record["avg_entropy"] = avg_entropy
        yield word, records


class PropagateOrdSumToGroup(beam.DoFn):
    """Meaningless function to simulate some workload."""

    def process(self, element):
        word, records = element
        avg_ord_sum = sum(ord_sum(record["uuid"]) for record in records) / len(records)
        for record in records:
            record["avg_ord_sum "] = avg_ord_sum
        yield word, records


class Enrich(beam.PTransform):
    def expand(self, word_records):
        return (
            word_records
            | "PropagateEntropyToGroup" >> beam.ParDo(PropagateEntropyToGroup())
            | "PropagateOrdSumToGroup" >> beam.ParDo(PropagateOrdSumToGroup())
        )


class UnpackFromKey(beam.DoFn):
    def process(self, word_records):
        word, records = word_records
        for record in records:
            yield record


def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input",
        dest="input",
        default="gs://dataflow-samples/shakespeare/kinglear.txt",
        help="Input file to process.",
    )
    parser.add_argument(
        "--output",
        dest="output",
        required=True,
        help="Output file to write results to.",
    )
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    with beam.Pipeline(options=pipeline_options) as p:
        lines = p | "Read" >> ReadFromText(known_args.input)

        word_records = lines | "GenerateWordRecords" >> beam.ParDo(
            GenerateWordRecords()
        )

        enriched_records = (
            word_records
            | "GroupByKey" >> beam.GroupByKey()
            | "Enrich" >> Enrich()
            | "UnpackFromKey" >> beam.ParDo(UnpackFromKey())
        )

        enriched_records | "Write" >> WriteToText(known_args.output)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
