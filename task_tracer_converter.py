#!/usr/bin/env python
# -*- coding: utf-8 -*-


import sys
import argparse
import json
from sets import Set


class Task(object):
  def __init__(self, task_id):
    super(Task, self).__init__()

    # Property _task_id is required for each object of Task
    self._task_id = task_id

    # Basic information
    self._source_event_id = 0
    self._source_event_type = 0
    self._process_id = 0
    self._thread_id = 0

    # Timestamp information
    self._dispatch = 0
    self._start = 0
    self._end = 0

  @property
  def dispatch(self):
    return self._dispatch

  @dispatch.setter
  def dispatch(self, timestamp):
    self._dispatch = timestamp

  @property
  def start(self):
    return self._start

  @start.setter
  def start(self, timestamp):
    self._start = timestamp

  @property
  def end(self):
    return self._end

  @end.setter
  def end(self, timestamp):
    self._end = timestamp

  def set_basic_info(self, info):
    if len(info) != 4:
      return False

    self._source_event_id = int(info[0])
    self._source_event_type = int(info[1])
    self._process_id = int(info[2])
    self._thread_id = int(info[3])
    return True

  def check_basic_info(self, info):
    if any( [len(info) != 4,
             int(info[0]) != self._source_event_id,
             int(info[1]) != self._source_event_type,
             int(info[2]) != self._process_id,
             int(info[3]) != self._thread_id] ):
      return False

    return True

def parse_log(input_name):
  log_file = open(input_name, 'r')
  num_line = 0

  for line in log_file:
    # [tag, task_id, source_event_id, source_event_type, process_id,
    #  thread_id, action_type, timestamp, (customized info)]
    tokens = line.split()
    if len(tokens) < 8:
      print 'Parse error: incomplete data (', line, ')'
      return None

    timestamp = int(tokens[7])
    action_type = int(tokens[6])
    if not action_type in (0, 1, 2):
      print 'Parse error: invalid action type (', action_type, ')'
      log_file.close()
      return None

    num_line += 1
    task_id = tokens[1]
    if task_id not in data:
      data[task_id] = Task(int(task_id));
      data[task_id].set_basic_info(tokens[2:6])
#    else:
#      if not data[task_id].check_basic_info(tokens[2:6]):
#        print 'Parse error: inconsistent data (', line, ')'
#        del data[task_id]
#        log_file.close()
#        return None

    if action_type == 0:
      data[task_id].dispatch = timestamp
    elif action_type == 1:
      data[task_id].start = timestamp
    else:
      data[task_id].end = timestamp

  log_file.close()
  return num_line

def output_json(output_name):
  all_timestamps = Set([])
  for task_id, task_object in data.iteritems():
    all_timestamps.add(task_object.dispatch)
    all_timestamps.add(task_object.start)
    all_timestamps.add(task_object.end)

  # The initial value for these timestamps is 0, so we have to remove it.
  if 0 in all_timestamps:
    all_timestamps.remove(0)

  output_file = open(output_name, 'w')
  output_file.write('{\"start\": %d, \"end\": %d, \"tasks\":'
                    % (min(all_timestamps), max(all_timestamps)))
  output_file.write(json.dumps(data.values(), default=lambda o: o.__dict__,
                               indent=4))
  output_file.write('}')
  output_file.close()

def get_arguments(argv):
  parser = argparse.ArgumentParser()
  parser.add_argument('-i', '--input-file', help='Input file', required=True)
  parser.add_argument('-o', '--output-file', help='Output file (Optional)',
                      default='task_tracer_data.json', required=False)
  return parser.parse_args()

def main(argv=sys.argv[:]):
  args = get_arguments(argv)
  print 'Input:', args.input_file
  print 'Output:', args.output_file

  if not parse_log(args.input_file):
    sys.exit()
  print len(data), 'tasks has been created successfully.'

  output_json(args.output_file)
  print len(data), 'tasks has been written to JSON output successfully.'

data = {}

if __name__ == '__main__':
  sys.exit(main())

