#!/usr/bin/env python
# -*- coding: utf-8 -*-


import sys
import argparse
import json
from sets import Set
import sqlite3

class Label(object):
  def __init__(self, timestamp, label):
    self.timestamp = timestamp
    self.label = label

class Task(object):
  def __init__(self, task_id):
    super(Task, self).__init__()

    # Property taskId is required for each object of Task
    self.taskId = task_id

    self.sourceEventId = None
    self.sourceEventType = None
    self.processId = None
    self.threadId = None
    self.parentTaskId = None

    # Timestamp information
    self.dispatch = 0
    self.start = 0
    self.end = 0

    self.labels = []

  def add_label(self, timestamp, label):
    label = Label(timestamp, label)
    self.labels.append(label);

def extract_info(log):
  # Example:
  # I/TaskTracer( 1570): 1 6743098656705 316475166353175 6743098656704 1 6743098656704
  # I/TaskTracer( 1570): 2 6743098656705 316475166399378 1570 1570
  # I/TaskTracer( 1570): 3 6743098656705 316475166399592
  # I/TaskTracer( 1570): 4 6743098656705 316475166399400 "HelloWorld label"

  # Remove dummy log like '--------- beginning of ...'
  if log.startswith('-'):
    return None

  # Remove tag name. Ex. 'I/TaskTracer( 1570):'
  [tag, log_no_tag] = log.split(':', 1)

  # Retrieve log type
  [log_type, log_no_tag_no_type] = log_no_tag.strip().split(' ', 1)
  log_type = int(log_type)
  if not log_type in range(0, 6):
    print 'Parse error: invalid log type (', log_type, ')'
    return False

  info = None
  if log_type == 4:
    [taskId, timestamp, label] = log_no_tag_no_type.strip().split(' ', 2)
    info = [log_type, taskId, timestamp, label.split('"')[1]]
  else:
    info = log_no_tag.split()

  if any(((log_type == 1) and (len(info) != 6),
          (log_type == 2) and (len(info) != 5),
          (log_type == 3) and (len(info) != 3),
          (log_type == 4) and (len(info) != 4))):
    print 'Parse error: incomplete data (', log.strip(), ')'
    return False

  return info

def parse_log(input_name, show_warnings):
  num_line = 0

  # Read log
  with open(input_name, 'r') as log_file:
    all_log = log_file.readlines()

  # Parse log line by line
  for line in all_log:
    info = extract_info(line)

    if info is False:
      return False
    elif not info:
      continue

    num_line += 1

    log_type = int(info[0])
    # TODO
    if log_type == 5:
      if show_warnings:
        print ('Skip since the feature haven\'t completed yet. \'' +
               line.strip() + '\'');
      continue

    task_id = info[1]
    if task_id not in data:
      if log_type == 1:
        data[task_id] = Task(int(task_id))
      else:
        if show_warnings:
          print ('Skip because of incomplete logs. \'' +
                 line.strip() + '\'')
        continue

    # CREATE:   [0, sourceEventId, create, ...]
    # DISPATCH: [1, taskId, dispatch, sourceEventId, sourceEventType, parentTaskId]
    # START:    [2, taskId, start, processId, threadId]
    # END:      [3, taskId, end]
    # LABEL:    [4, taskId, timestamp, label]
    # VTABLE:   [5, taskId, vtable]
    timestamp = int(info[2])
    if log_type == 1:
      data[task_id].dispatch = timestamp
      data[task_id].sourceEventId = int(info[3])
      data[task_id].sourceEventType = int(info[4])
      data[task_id].parentTaskId = int(info[5])
    elif log_type == 2:
      data[task_id].start = timestamp
      data[task_id].processId = int(info[3])
      data[task_id].threadId = int(info[4])
    elif log_type == 3:
      data[task_id].end = timestamp
    elif log_type == 4:
      data[task_id].add_label(int(info[2]), info[3])

  return True

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
    if task_object.start == 0:
      task_object.start = profiler_end_time
    if task_object.end == 0:
      task_object.end = profiler_end_time

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
                      default='task_tracer_data.json')
  parser.add_argument('-c', '--check-parent-task-id', action='store_const',
                      const=True, help='Check parentTaskId (if possible)')
  parser.add_argument('-w', '--show-warnings', action='store_const', const=True,
                      help='Show warnings')
  parser.add_argument('-p', '--print-all-tasks', action='store_const',
                      const=True, help='Print all tasks')
  return parser.parse_args()

def create_table_and_insert_data():
  print 'Create database \'task_tracer.db\'.'
  conn = sqlite3.connect('task_tracer.db')
  with conn:
    cur = conn.cursor()

    # Delete the table if exists and re-create the table
    cur.execute('DROP TABLE IF EXISTS Tasks')
    cur.execute(('CREATE TABLE Tasks('
                 'taskId INT, threadId INT, start INT, end INT)'))

    # Insert information into table
    for task_id, task_obj in data.iteritems():
      # Only tasks which includes complete information are inserted into database
      if any((task_obj.threadId is None,
              task_obj.start is 0,
              task_obj.end is 0)):
        continue

      insert_cmd = ('INSERT INTO Tasks VALUES({}, {}, {}, {})'.format(task_id,
                    task_obj.threadId, task_obj.start, task_obj.end))
      cur.execute(insert_cmd)

    cur.execute('SELECT * FROM Tasks')
    num_records = len(cur.fetchall())
    print 'Insert {} records into Table Tasks'.format(num_records)

def check_parent_task_id():
  create_table_and_insert_data()

  conn = sqlite3.connect('task_tracer.db')
  num_no_result = 0
  num_multi_results = 0
  num_error_result = 0

  with conn:
    cur = conn.cursor()

    # Verify parentTaskId with query results
    for task_id, task_obj in data.iteritems():
      if task_obj.dispatch is 0:
        continue

      # Retrieve threadId from taskId
      thread_id = int(task_id) >> 32
      select_cmd = ('SELECT taskId FROM Tasks ' +
                    'WHERE threadId={} AND '.format(thread_id) +
                    'start<={} AND '.format(task_obj.dispatch) +
                    'end>={}'.format(task_obj.dispatch))
      cur.execute(select_cmd)

      rows = cur.fetchall()
      if len(rows) is 0:
        num_no_result += 1
        continue
      elif len(rows) > 1:
        num_multi_results += 1
        continue
      elif task_obj.parentTaskId != rows[0][0]:
        print ("Verify error: inconsistent 'parentTaskId'. Input: " +
               '{}, Query result: {}'.format(task_obj.parentTaskId, rows[0][0]))
        num_error_result += 1
        continue

  num_total_tasks = float(len(data))
  num_verified_task = len(data) - num_no_result - \
                      num_multi_results - num_error_result
  print ('{} tasks ({}%) failed to query parentTaskId in database.\n'.
         format(num_no_result, float(num_no_result)/num_total_tasks) +
         '{} tasks ({}%) got multiple results for parentTaskId in database.\n'.
         format(num_multi_results, float(num_multi_results)/num_total_tasks) +
         '{} tasks ({}%) got inconsistent parentTaskId in database.\n'.
         format(num_error_result, float(num_error_result)/num_total_tasks) +
         '{} tasks ({}%) are verified.'.
         format(num_verified_task, float(num_verified_task)/num_total_tasks))

def print_all_tasks():
  for task_id, task_obj in data.iteritems():
    labels_str = json.dumps(task_obj.labels, default=lambda o: o.__dict__)
    print ('taskId: {}, '.format(task_id) +
           'sourceEventType: {}, '.format(task_obj.sourceEventType) +
           'sourceEventId: {}, '.format(task_obj.sourceEventId) +
           'processId: {}, '.format(task_obj.processId) +
           'threadID: {}, '.format(task_obj.threadId) +
           'parentTaskId: {}, '.format(task_obj.parentTaskId) +
           'dispatch: {}, '.format(task_obj.dispatch) +
           'start: {}, '.format(task_obj.start) +
           'end: {}, '.format(task_obj.end) +
           'labels: {}'.format(labels_str))

def main(argv=sys.argv[:]):
  args = get_arguments(argv)

  print 'Input:', args.input_file
  print 'Output:', args.output_file

  if parse_log(args.input_file, args.show_warnings) is False:
    sys.exit()
  print len(data), 'tasks has been created successfully.'

  if args.check_parent_task_id:
    check_parent_task_id()

  [profiler_start_time, profiler_end_time] = retrieve_profiler_start_end_time();
  replace_undefined_timestamp(profiler_start_time, profiler_end_time);

  output_json(args.output_file, profiler_start_time, profiler_end_time)

  print len(data), 'tasks has been written to JSON output successfully.'
  if args.print_all_tasks:
    print_all_tasks()

data = {}

if __name__ == '__main__':
  sys.exit(main())

