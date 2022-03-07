import unittest

import apache_beam as beam
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.test_pipeline import TestPipeline


from task2 import CompositeTransform


class Task2Test(unittest.TestCase):

    def test_task2_filter_amount(self):
        """Test case for amount filer."""
        ROWS = [
            '2017-01-01 04:22:23 UTC,wallet00000e719adfeaa64b5a,wallet00001e494c12b3083634,19.95']
        p = TestPipeline()
        input = (p | beam.Create(self.ROWS))
        output = (input | CompositeTransform())
        assert_that(
            output,
            equal_to([]))
        p.close()

    def test_task2_filter_year(self):
        """Test case for year filer."""
        ROWS = [
            '2009-01-01 04:22:23 UTC,wallet00000e719adfeaa64b5a,wallet00001e494c12b3083634,19.95']
        p = TestPipeline()
        input = (p | beam.Create(self.ROWS))
        output = (input | CompositeTransform())
        assert_that(
            output,
            equal_to([]))
        p.close()

    def test_task2_all_cases(self):
        """Test case for all cases"""
        ROWS = ['2009-01-09 02:54:25 UTC,wallet00000e719adfeaa64b5a,wallet00001866cb7e0f09a890,1021101.99',
                '2017-01-01 04:22:23 UTC,wallet00000e719adfeaa64b5a,wallet00001e494c12b3083634,19.95',
                '2017-03-18 14:09:16 UTC,wallet00001866cb7e0f09a890,wallet00001e494c12b3083634,2102.22',
                '2017-03-18 14:10:44 UTC,wallet00001866cb7e0f09a890,wallet00000e719adfeaa64b5a,1.00030',
                '2018-02-27 16:04:11 UTC,wallet00005f83196ec58e4ffe,wallet00001866cb7e0f09a890,129.12']
        p = TestPipeline()
        input = (p | beam.Create(self.ROWS))
        output = (input | CompositeTransform())
        assert_that(
            output,
            equal_to(['2017-03-18, 2102.22', '2018-02-27, 129.12']))
        p.close()


if __name__ == '__main__':
    unittest.main()
