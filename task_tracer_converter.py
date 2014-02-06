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

def find_char_and_split(string, char=' ', num=-1):
  """
    Find the delimiter first and then split the string with the delimiter.

    returns:
      A list of the words in the string after spliting the string.
      None when failed to find the delimiter in the string.
  """
  if string.find(char) == -1:
    print 'Extract error: failed to find \'', char, '\'', ' in \'', string, '\''
    return None

  if num is -1:
    num = len(string)

  return string.split(char, num)

def extract_info(log):
  """
    Discard redundant message and extract information.

    log: Raw log.
      Ex. I/TaskTracer( 1570): 1 6743098656705 316475166353175 6743098656704 1 6743098656704

    returns:
      A list is returned after successfully extract information from input.
      None is returned if the input is redundant.
      False is returned  when an error occurred.
  """
  # Remove dummy log like '--------- beginning of ...'
  if log.startswith('-'):
    return None

  # Remove tag name. Ex. 'I/TaskTracer( 1570):'
  tokens = find_char_and_split(log, ':', 1)
  if not tokens: return False
  log = tokens[1].strip()

  # Retrieve log type
  tokens = find_char_and_split(log, ' ', 1)
  if not tokens: return False
  log_type = int(tokens[0])
  log = tokens[1].strip()

  # Retrieve other information
  info = None
  if log_type == 4:
    tokens = find_char_and_split(log, '"', 2)
    if not tokens: return False
    log = tokens[0]
    label = tokens[1]

    tokens = find_char_and_split(log)
    if not tokens: return False
    info = [log_type] + tokens[0:2] + [label]
  else:
    tokens = find_char_and_split(log)
    if not tokens: return False
    info = [log_type] + tokens

  return info

def verify_info(info):
  """
    Verify information value and format.

    info: A list of information.

    returns:
      True when verify passed.
      False when verify failed.
  """
  log_type = int(info[0])
  if not log_type in range(1, 6):
    print 'Verify error: invalid log type (', log_type, ')'
    return False

  if any(((log_type == 1) and (len(info) != 6),
          (log_type == 2) and (len(info) != 5),
          (log_type == 3) and (len(info) != 3),
           (log_type == 4) and (len(info) != 4))):
     print 'Verify error: incomplete data ', info
     return False

  return True

def set_task_info(info):
  """
    Set task properties based on log_type.

    info: A list of information. Each type has its own format.
      DISPATCH: [1 taskId dispatch sourceEventId sourceEventType parentTaskId]
      START:    [2 taskId start processId threadId]
      END:      [3 taskId end]
      LABEL:    [4 taskId timestamp, label]
      VTABLE:   [5 taskId vtable]
  """
  log_type = int(info[0])

  # TODO
  if log_type == 5:
    if show_warnings:
      print ('Skip since the feature haven\'t completed yet. \'' +
             line.strip() + '\'');
    return

  task_id = info[1]
  if task_id not in data:
    if log_type == 1:
      data[task_id] = Task(int(task_id))
    else:
      if show_warnings:
        print 'Skip because of incomplete logs. \'', line.strip(), '\''
      return

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

def parse_log(input_name):
  """
    Parse input file line by line.

    input_name: Input filename.

    returns:
      True if the whole file is parsed successfully.
      False when error occurred.
  """
  with open(input_name, 'r') as log_file:
    all_log = log_file.readlines()

  for line in all_log:
    info = extract_info(line.strip())
    if not info:
      continue
    elif info is False:
      return False

    result = verify_info(info)
    if not result:
      continue
    elif result is False:
      return False

    set_task_info(info)

  return True

def retrieve_start_end_time():
  """Scan through all timestamps and return the min and the max."""
  all_timestamps = Set([])
  for task_id, task_object in data.iteritems():
    all_timestamps.add(task_object.dispatch)
    all_timestamps.add(task_object.start)
    all_timestamps.add(task_object.end)

  # The initial value for these timestamps is 0, so we have to remove it.
  if 0 in all_timestamps:
    all_timestamps.remove(0)

  return [min(all_timestamps), max(all_timestamps)]

def replace_undefined_timestamp(end_time):
  """Replace undefined timestamp with the max of all timestamps."""
  for task_id, task_object in data.iteritems():
    if task_object.start == 0:
      task_object.start = end_time
    if task_object.end == 0:
      task_object.end = end_time

def output_json(output_name, start_time, end_time):
  """
    Write data out in JSON format.

    output_name: Output filename.
    start_time: the min of all timestamps.
    end_time: the max of all timestamps.
  """
  output_file = open(output_name, 'w')
  output_file.write('{\"start\": %d, \"end\": %d, \"tasks\":'
                    % (start_time, end_time))
  output_file.write(json.dumps(data.values(), default=lambda o: o.__dict__,
                               indent=4))
  output_file.write('}')
  output_file.close()

def get_arguments(argv):
  """Parse arguments."""
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
  """Create a database and insert all tasks into a table."""
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
  """
    Prepare a database first, and verify the value of parentTaskId.

    A taskId consist of its threadId and a serial number.
    |    threadId     |   serial num.   |
    | <-- 32 bits --> | <-- 32 bits --> |

    Additionally, the child task is dispatched during the execution time of its
    parent task. And, for each thread, tasks should be executed once at a time.
    Thus, only ONE task should be returned when we query the database with the
    following  conditions:
      1. parent.threadId = high_bits(child.taskId)
      2. parent.start < child.dispatch
      3. parent.end > child.dispatch
  """
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

      # Retrieve the 32 high-order bits
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
  """Print task information in text mode."""
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

  show_warnings = args.show_warnings
  if parse_log(args.input_file) is False:
    sys.exit()
  print len(data), 'tasks has been created successfully.'

  if args.check_parent_task_id:
    check_parent_task_id()

  [start_time, end_time] = retrieve_start_end_time();
  replace_undefined_timestamp(end_time);

  output_json(args.output_file, start_time, end_time)

  print len(data), 'tasks has been written to JSON output successfully.'
  if args.print_all_tasks:
    print_all_tasks()

data = {}
show_warnings = False

if __name__ == '__main__':
  sys.exit(main())

