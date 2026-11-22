import unittest

from spark.certificates_streaming import parse_args


class CertificateStreamArgsTest(unittest.TestCase):
    def test_defaults_to_certificate_output_and_latest_offsets(self):
        args = parse_args([])

        self.assertEqual(args.output_topic, "cyber-certs-processed")
        self.assertEqual(args.starting_offsets, "latest")

    def test_rejects_output_to_input_topic(self):
        with self.assertRaises(SystemExit):
            parse_args(["--output-topic", "cyber-certs"])
