#!/usr/bin/env python

"""
Stitch multiple files worth of AWS transcripts together.
Does not attempt to match speakers across filesm but does label all speaker changes.
Usage:

    python stitch_transcript.py *.mp3.json -o out.txt

See blog post: http://turtlemonvh.github.io/aws-transcribe-for-long-zoom-meetings.html

"""

import datetime
import json


class SpeakerSection(object):
    def __init__(self, o, filename):
        self.start_time = float(o.get('start_time', 0))
        self.end_time = float(o.get('end_time', 0))
        self.speaker = o['speaker_label']
        self.item_count = len(o['items'])
        self.filename = filename


class ItemSection(object):
    def __init__(self, o):
        self.start_time = o.get('start_time')
        if self.start_time is not None:
            self.start_time = float(self.start_time)
        self.end_time = o.get('end_time')
        if self.end_time is not None:
            self.end_time = float(self.end_time)
        self.alternatives = o['alternatives']
        self.item_type = o['type']

        if self.start_time is None or self.end_time is None:
            if not self.is_punctuation:
                raise Exception("Unexpected item format: %s" % (o))

    @property
    def is_punctuation(self):
        return self.item_type == u"punctuation"


class StitchedContent(object):
    def __init__(self, speaker_label, item_sections, time_offset):
        self.speaker_label = speaker_label
        self.item_sections = item_sections
        self.time_offset = time_offset

    def append(self, item_section):
        self.item_sections.append(item_section)

    def render(self):
        content = ""
        for item_section in self.item_sections:
            if len(content) > 0 and item_section.item_type != "punctuation":
                content += " "
            content += item_section.alternatives[0]['content']

        # [ speaker part#.speaker# ] : ( starttime - endtime )
        header = "[ speaker {}:{} ] : ( {} - {} )".format(
            self.speaker_label.speaker,
            self.speaker_label.filename,
            format_duration(self.speaker_label.start_time + self.time_offset),
            format_duration(self.speaker_label.end_time + self.time_offset)
        )

        return "{}\n{}".format(
            header,
            content
        )


def format_duration(seconds):
    """
    Takes a timedelta and formats
    """
    td = datetime.timedelta(seconds=seconds)
    return "{:02d}:{:02d}:{:02d}:{:02d}".format(td.seconds, (td.seconds % 3600) % 60, td.seconds % 60,
                                                td.microseconds % 1000)


def get_result_speaker_sections(d, filename):
    for item in d['results']['speaker_labels']['segments']:
        yield SpeakerSection(item, filename)


def get_result_item_sections(d):
    for item in d['results']['items']:
        yield ItemSection(item)


def assert_iterator_empty(itr):
    try:
        next(itr)
    except StopIteration:
        pass
    else:
        assert False


def stitch(files, debug=False):
    time_offset = 0
    max_offset = 0

    for f in files:
        with open(f) as tf:
            d = json.load(tf)

        time_offset = time_offset + max_offset

        speaker_sections = get_result_speaker_sections(d, f)
        items_sections = get_result_item_sections(d)

        speaker_section = next(speaker_sections)
        item_section = next(items_sections)
        current_stitched_section = StitchedContent(speaker_section, [], time_offset)

        while True:
            try:
                if item_section.is_punctuation or item_section.end_time <= speaker_section.end_time:
                    # Same speaker
                    if not item_section.is_punctuation:
                        assert (item_section.start_time >= speaker_section.start_time)
                    current_stitched_section.append(item_section)
                    item_section = next(items_sections)
                else:
                    # New speaker
                    prev_speaker_section = speaker_section
                    speaker_section = next(speaker_sections)
                    if prev_speaker_section.speaker != speaker_section.speaker:
                        yield current_stitched_section
                        current_stitched_section = StitchedContent(speaker_section, [], time_offset)
                    continue
            except StopIteration:
                # Check that they are both empty
                if debug:
                    print("speaker_section", speaker_section.__dict__)
                    print("item_section", item_section.__dict__)

                max_offset = max(speaker_section.end_time, item_section.end_time)
                assert_iterator_empty(speaker_sections)
                assert_iterator_empty(items_sections)
                yield current_stitched_section
                break


if __name__ == "__main__":

    import argparse

    p = argparse.ArgumentParser(description="Stitch together AWS Transcribe JSON output, esp. across multiple chunks.")
    p.add_argument('files', action='store', nargs='+', help='Pattern of files to pick up.')
    p.add_argument('-o', '--outputfile', action='store', help='Filename to write to.')

    options = p.parse_args()
    with open(options.outputfile, "w+") as outputfile:
        for i, stitched_content in enumerate(stitch(options.files)):
            outputfile.write(stitched_content.render() + "\n")
