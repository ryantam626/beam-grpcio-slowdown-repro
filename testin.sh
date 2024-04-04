python -m apache_beam.examples.wordcount \
    --region europe-west1 \
    --input gs://dataflow-samples/shakespeare/kinglear.txt \
    --output gs://huq-dhruv-karan-beam-tests/results/outputs \
    --runner DataflowRunner \
    --project huq-dhruv-karan \
    --temp_location gs://huq-dhruv-karan-beam-tests/tmp/
