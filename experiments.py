#!/usr/bin/env python2

# This script serves to extract experiments results from the
# experiment log files

import re
from sys import stderr, argv

class Experiment():
    file_name_pattern = re.compile(r".*/(\w+)/flood-ball-dec_-p_(\d+\.\d+).*-r_(\d+)/(\d+).*")
    decomposition_time_pattern = re.compile(r"^flood-ball-decomposition (\d+)")
    diameter_time_pattern = re.compile(r"^hyperANF (\d+)")
    cardinality_pattern = re.compile(r"^cardinality (\d+)")
    diameter_pattern = re.compile(r"^diameter (\d+)")

    def decompose_file_name(self):
        "decompose the file name to extract p, r and num_procs"
        match = self.file_name_pattern.match(self.log_file_name)
        if match:
            self.dataset = match.group(1)
            self.prob = float(match.group(2))
            self.radius = int(match.group(3))
            self.procs = int(match.group(4))
        else:
            raise ValueError("Invalid file name: %s\n" % self.log_file_name)

    def parse_timings(self):
        with open(self.log_file_name, 'r') as log_file:
            for line in log_file:
                match = self.decomposition_time_pattern.match(line)
                if match:
                    self.decomposition_time = int(match.group(1))
                match = self.diameter_time_pattern.match(line)
                if match:
                    self.diameter_time = int(match.group(1))
                match = self.cardinality_pattern.match(line)
                if match:
                    self.cardinality = int(match.group(1))
                match = self.diameter_pattern.match(line)
                if match:
                    self.diameter = int(match.group(1))

    def __init__(self, log_file_name):
        self.log_file_name = log_file_name
        self.decompose_file_name()
        self.parse_timings()

    def __str__(self):
        return """Experiment:
        dataset: %s
        prob: %.2f
        radius: %d
        procs: %d
        decomposition time: %d
        diameter time: %d
        cardinality: %d
        diameter: %d
        """ % (self.dataset, self.prob, self.radius, self.procs,
               self.decomposition_time, self.diameter_time,
               self.cardinality, self.diameter)

    @staticmethod
    def org_table_header():
        return "| dataset | procs | p | r | t_decomposition | t_diameter | cardinality | diameter |"

    def as_org_table_line(self):
        return "| %s | %d | %.2f | %d | %d | %d | %d | %d |" % (self.dataset, self.procs, self.prob, self.radius,
                                                                self.decomposition_time, self.diameter_time,
                                                                self.cardinality, self.diameter)

if __name__ == '__main__':
    print Experiment.org_table_header()
    for log_file in argv[1:]:
        try:
            print Experiment(log_file).as_org_table_line()
        except ValueError as e:
            stderr.write("%s - skipping %s\n" % (e, log_file))
        except AttributeError as e:
            stderr.write("%s - skipping %s\n" % (e, log_file))
