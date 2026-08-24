from __future__ import annotations

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.test_stream import TestStream as BeamTestStream
from apache_beam.testing.util import assert_that
from apache_beam.transforms.window import TimestampedValue
from apache_beam.utils.windowed_value import PaneInfoTiming


def test_teststream_accepts_late_event_as_accumulating_revision(solution):
    stream = (
        BeamTestStream()
        .advance_watermark_to(0)
        .add_elements([TimestampedValue(("m-a", 10), 5)])
        .advance_watermark_to(61)
        .add_elements([TimestampedValue(("m-a", 20), 50)])
        .advance_watermark_to_infinity()
    )

    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True

    def check_panes(actual):
        observed = {
            (item.value[0], item.value[1], item.pane_info.timing)
            for item in actual
        }

        assert ("m-a", 10, PaneInfoTiming.ON_TIME) in observed
        assert ("m-a", 30, PaneInfoTiming.LATE) in observed

    with BeamTestPipeline(options=options) as pipeline:
        totals = (
            pipeline
            | "Temporal input" >> stream
            | "Temporal policy"
            >> solution.build_trigger_policy(
                window_seconds=60,
                allowed_lateness_seconds=120,
            )
            | "Sum temporal totals" >> beam.CombinePerKey(sum)
        )

        assert_that(
            totals,
            check_panes,
            reify_windows=True,
            label="Check on-time and late panes",
        )