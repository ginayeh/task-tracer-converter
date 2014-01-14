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

    self._source_event_id = None
    self._source_event_type = None
    self._process_id = None
    self._thread_id = None

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

  @property
  def sourceEventId(self):
    return self._source_event_id

  @sourceEventId.setter
  def sourceEventId(self, source_event_id):
    self._source_event_id = source_event_id

  @property
  def sourceEventType(self):
    return self._source_event_type

  @sourceEventType.setter
  def sourceEventType(self, source_event_type):
    self._source_event_type = source_event_type

  @property
  def processId(self):
    return self._process_id

  @processId.setter
  def processId(self, process_id):
    self._process_id = process_id

  @property
  def threadId(self):
    return self._thread_id

  @threadId.setter
  def threadId(self, thread_id):
    self._thread_id = thread_id

def parse_log(input_name):
  log_file = open(input_name, 'r')
  num_line = 0

  for line in log_file:
    tokens = line.split()
    if (len(tokens) < 4):
      print 'Parse error: incomplete data (', line, ')'
      log_file.close()
      return None

    # [tag, log_type, task_id, timestamp, ...]
    log_type = int(tokens[1])
    task_id = tokens[2]
    timestamp = int(tokens[3])

    if not log_type in (1, 2, 3):
      print 'Parse error: invalid log type (', log_type, ')'
      log_file.close()
      return None

    if any(((log_type == 1) and (len(tokens) != 6),
           (log_type == 2) and (len(tokens) != 6),
           (log_type == 3) and (len(tokens) != 4))):
        print 'Parse error: incomplete data (', line, ')'
        log_file.close()
        return None

    num_line += 1

    if task_id not in data:
      data[task_id] = Task(int(task_id))

    if log_type == 1:
      # [tag, log_type, task_id, dispatch, sourceEventId, sourceEventType]
      data[task_id].dispatch = timestamp
      data[task_id].sourceEventId = int(tokens[4])
      data[task_id].sourceEventType = int(tokens[5])
    elif log_type == 2:
      # [tag, log_type, task_id, start, processId, threadId]
      data[task_id].start = timestamp
      data[task_id].processId = int(tokens[4])
      data[task_id].threadId = int(tokens[5])
    else:
      # [tag, log_type, task_id, end]
      data[task_id].end = timestamp

  log_file.close()
  return num_line

def retrieve_profiler_start_end_time():
  all_timestamps = Set([])
  for task_id, task_object in data.iteritems():
    all_timestamps.add(task_object.dispatch)
    all_timestamps.add(task_object.start)
    all_timestamps.add(task_object.end)

  # The initial value for these timestamps is 0, so we have to remove it.
  if 0 in all_timestamps:
    all_timestamps.remove(0)

  return [min(all_timestamps), max(all_timestamps)]

def replace_undefined_timestamp(profiler_start_time, profiler_end_time):
  for task_id, task_object in data.iteritems():
    if task_object.dispatch == 0:
      task_object.dispatch = profiler_start_time
      if task_object.start == 0:
        task_object.start = profiler_start_time

    if task_object.end == 0:
      task_object.end = profiler_end_time
      if task_object.start == 0:
        task_object.start = profiler_end_time

def output_json(output_name, profiler_start_time, profiler_end_time):
  output_file = open(output_name, 'w')
  output_file.write('{\"start\": %d, \"end\": %d, \"tasks\":'
                    % (profiler_start_time, profiler_end_time))
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

  [profiler_start_time, profiler_end_time] = retrieve_profiler_start_end_time();
  replace_undefined_timestamp(profiler_start_time, profiler_end_time);
  output_json(args.output_file, profiler_start_time, profiler_end_time)
  print len(data), 'tasks has been written to JSON output successfully.'

data = {}

if __name__ == '__main__':
  sys.exit(main())

