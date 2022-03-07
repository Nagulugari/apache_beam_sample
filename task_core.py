"""This module defines classes required for the task."""
from datetime import datetime

import apache_beam as beam


class ExtractFields(beam.DoFn):
    """Returns list of tuples those are parsed date and amount values. """

    def process(self, element):
        if element:
            fields = element.split(',')
            date = self._parse_date(fields[0])
            amount = float(fields[3])
            return [(date, amount)]

    def _parse_date(self, input_value):
        if input_value:
            return input_value[0:10]


class AmountFilter(beam.DoFn):
    """Filters the records with amount field. """

    def process(self, record):
        if record:
            if record[1] > 20:
                return [record]


class DateFilter(beam.DoFn):
    """Filters the records with Date field. """

    def process(self, record):
        if record:
            if self._get_year(record[0]) > 2010:
                return [record]

    def _get_year(self, date_str):
        if date_str:
            date = datetime.strptime(date_str, '%Y-%m-%d')
            return date.year


class FormatFields(beam.DoFn):
    """Formates the records as csv. """

    def process(self, key):
        res = f'{key[0]}, {key[1]}'
        return [res]
