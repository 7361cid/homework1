import unittest
from log_analyzer_reduced import parse_log_record


class TestUtilDate(unittest.TestCase):
    def test_parse_log_record(self):
        log_line = b'1.200.76.128 f032b48fb33e1e692  - [29/Jun/2017:11:05:55 +0300] ' \
                   b'"GET /api/1/campaigns/?id=984781 HTTP/1.1" 200 662 ' \
                   b'"-" "-" "-" "1498723554-4102637017-4708-9976726" "-" 1.163\n'
        self.assertEqual(parse_log_record(log_line), ('/api/1/campaigns/?id=984781', '1.200'))


if __name__ == '__main__':
    unittest.main()
